package com.xbstar.chenguang;

import com.xbstar.FlinkTableSchema;
import com.xbstar.CabbageUtils;
import com.xbstar.chenyiming.DMLBean;
import com.xbstar.chenyiming.DMLogSQLParser;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DMSourceFunction<T> extends RichSourceFunction<T> implements CheckpointedFunction
{
	// 取消控制参数
	private boolean cancel = true;
	// 返回类型参数
	private Class<T> recordClass;
	// 数据库配置参数
	private FlinkTableSchema tableSchema; //FlinkTable的Row行数据类型，支持序列化
	private final int port;
	private final String hostName;
	private final String userName;
	private final String password;
	private final String databaseName; //配置为达梦的模式名
	private final String tableName;
	// 数据库连接变量
	private Connection dmDataBaseConnection;
	private Statement dmQuerySelector;
	// 数据库增量捕获的参数
	private ListState<Long> dmIncrementLSN;

	DMSourceFunction(int port, String hostName, String userName, String password, String databaseName, String tableName, FlinkTableSchema resolvedSchema, Class<T> recordClass)
	{
		// 初始化链接参数
		this.port = port;
		this.hostName = hostName;
		this.userName = userName;
		this.password = password;
		this.databaseName = databaseName;
		this.tableName = tableName;
		this.recordClass = recordClass;
		if (tableSchema != null)
		{
			// 用户传入了Schema,按照用户的要求配置
			this.tableSchema = resolvedSchema;
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception
	{
		ListStateDescriptor<Long> lsnDescriptor = new ListStateDescriptor<Long>("LSN", Long.class);
		this.dmIncrementLSN = functionInitializationContext.getOperatorStateStore().getListState(lsnDescriptor);
	}

	@Override
	public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception
	{
		// 在execIncrement中每次读取记录都保存了LSN到State中这里不用做任何事
	}

	@Override
	public void open(Configuration parameters) throws Exception
	{
		// 建立达梦数据库连接
		Class.forName("dm.jdbc.driver.DmDriver");
		String url = "jdbc:dm://" + hostName + ":" + port;
		this.dmDataBaseConnection = DriverManager.getConnection(url, userName, password);
		this.dmDataBaseConnection.setAutoCommit(true); //设置自动提交
		this.dmQuerySelector = this.dmDataBaseConnection.createStatement();
		if (this.tableSchema == null)
		{
			// 如果用户没有传入配置的字段，则从JDBC中读取
			this.tableSchema = CabbageUtils.getTableSchemaFromDM(this.dmDataBaseConnection, tableName);
		}
		else
		{
			// 如果用户传入了Schema则进行比对，配置Schema中的字段应该是JDBC读入字段的子集
			this.compareSchema();
		}
	}

	private void compareSchema() throws SQLException
	{
		FlinkTableSchema declareSchema = this.tableSchema;
		FlinkTableSchema jdbcSchema = CabbageUtils.getTableSchemaFromDM(this.dmDataBaseConnection, tableName);
		/*字段上声明的字段必须是数据库字段的子集
		 * 另外因为大小写不敏感，统一转化为大写进行比较
		 */
		Set<String> jdbcColumnNames = jdbcSchema.getRowColumnNames().stream().map(name -> name.toUpperCase()).collect(Collectors.toSet());
		for (String declareColumn : declareSchema.getRowColumnNames())
		{
			if (!jdbcColumnNames.contains(declareColumn.toUpperCase()))
			{
				System.out.println("\033[1;31m" + this.tableSchema
						.getTableName() + " 中声明的字段 " + declareColumn + " 在数据库表 " + tableName + " 中不存在" + "\033[0m");
				throw new IllegalArgumentException(this.tableSchema
						.getTableName() + " 中声明的字段 " + declareColumn + " 在数据库表 " + tableName + " 中不存在");
			}
		}
	}

	@Override
	public void run(SourceContext<T> sourceContext) throws Exception
	{
		// 判断模式和表是否存在，如果没有则抛出SQLException
		ResultSet countCursor = this.dmQuerySelector
				.executeQuery("SELECT count(*) FROM " + this.databaseName.toUpperCase() + "." + this.tableName.toUpperCase());
		countCursor.next();
		long recordCount = countCursor.getLong(1);
		// 根据是否能从Checkpoint中获取到LSN确定是否执行全量
		if (!this.dmIncrementLSN.get().iterator().hasNext())
		{
			// 获取不到那就是首次执行的情况，开始全量捕获
			System.out.println("\033[1;36m" + "DMSourceFunction开始执行DM数据库全量捕获，共需处理记录数=" + recordCount + "\033[0m");
			this.execFullLoad(sourceContext, this.recordClass);
			// 查询当前LSN保存到检查点
			ResultSet lsnCursor = this.dmQuerySelector.executeQuery("SELECT CUR_LSN from V$RLOG;");
			lsnCursor.next();
			long currentLSN = lsnCursor.getLong(1);
			this.dmIncrementLSN.clear();
			this.dmIncrementLSN.add(currentLSN);
		}
		// 然后不间断的执行无界增量
		Thread.sleep(1000);
		System.out.println("\033[1;36m" + "DMSourceFunction开始执行DM数据库增量捕获，链接到LSN=" + this.dmIncrementLSN.get().iterator().next() + "\033[0m");
		while (this.cancel)
		{
			execIncrement(sourceContext, this.recordClass);
			Thread.sleep(5000); //每5秒钟执行一次增量
		}
	}

	private String makeFullSelectSQL()
	{
		// 构造全量SQL语句
		StringBuilder builder = new StringBuilder();
		builder.append("SELECT ");
		Iterator<String> columnIt = this.tableSchema.getRowColumnNames().iterator();
		while (columnIt.hasNext())
		{
			builder.append(columnIt.next());
			if (columnIt.hasNext())
			{
				builder.append(",");
			}
			else
			{
				builder.append(" ");
			}
		}
		builder.append("FROM ");
		builder.append(this.databaseName);
		builder.append(".");
		builder.append(this.tableName);
		return builder.toString();
	}

	private void execFullLoad(SourceContext<T> collector, Class<T> rowClass) throws SQLException
	{
		// 执行SQL获得结果集
		ResultSet resultSet = this.dmQuerySelector.executeQuery(this.makeFullSelectSQL());
		while (resultSet.next())
		{
			// 数据行类型是插入
			Row row = Row.withNames(RowKind.INSERT);
			// 构造字段
			for (String columnName : this.tableSchema.getRowColumnNames())
			{
				DataType columnType = this.tableSchema.getDataTypeOfColumn(columnName);
				Object value = CabbageUtils.mapCDCValueToRowValue(columnType, resultSet.getObject(columnName), columnName);
				row.setField(columnName, value);
			}
			// 收集这一行记录
			collector.collect((T) (rowClass == Row.class ? row : this.tableSchema.convertRowToRowData(row)));
		}
	}

	private void execIncrement(SourceContext<T> collector, Class<T> rowClass) throws Exception
	{
		// 获得当前时间
		String currentTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
		// 查询并添加当前时间之后创建的或者当前处于Active状态的增量Redo日志
		ResultSet logFileSet = this.dmQuerySelector
				.executeQuery("SELECT PATH FROM SYS.V$ARCH_FILE WHERE CREATE_TIME > '" + currentTime + "' OR STATUS='ACTIVE'");
		if (!logFileSet.isBeforeFirst())
		{
			throw new RuntimeException("无法获得Redo日志，可能没有开启达梦数据库的RedoLog，请确认!");
		}
		while (logFileSet.next())
		{
			// 可能有多个匹配的日志需要一一加入
			String logFilePath = logFileSet.getString(1);
			Statement appendExecutor = this.dmDataBaseConnection.createStatement();
			appendExecutor.execute("DBMS_LOGMNR.ADD_LOGFILE('" + logFilePath + "')");
		}
		// 选择待分析的日志记录，只需要分析currentLSN之后的日志
		this.dmQuerySelector.execute("DBMS_LOGMNR.START_LOGMNR (options=>2130,startscn=>" + this.dmIncrementLSN.get().iterator().next() + ")");
		// 过滤并分析Redo日志，其中<=3是只过滤增删改操作，COMMIT_SCN不为空过滤了所有以提交的操作
		ResultSet redoLog = this.dmQuerySelector
				.executeQuery("SELECT SQL_REDO,COMMIT_SCN FROM V$LOGMNR_CONTENTS WHERE OPERATION_CODE <=3 AND SEG_OWNER='"
						+ databaseName.toUpperCase().toUpperCase()
						+ "' AND TABLE_NAME='" + tableName.toUpperCase() + "'");
		while (redoLog.next())
		{
			DMLBean dmlBean = DMLogSQLParser.parseSql(redoLog.getString(1));
			//System.out.println(dmlBean);
			switch (dmlBean.getOp())
			{
				case "INSERT":
				{
					Map<String, String> valMap = dmlBean.getAfter();
					Row row = Row.withNames(RowKind.INSERT);
					for (String columnName : this.tableSchema.getRowColumnNames())
					{
						DataType columnType = this.tableSchema.getDataTypeOfColumn(columnName);
						Object columnValue = CabbageUtils.mapCDCValueToRowValue(columnType, valMap.get(columnName), columnName);
						row.setField(columnName, columnValue);
					}
					collector.collect((T) (rowClass == Row.class ? row : this.tableSchema.convertRowToRowData(row)));
					break;
				}
				case "UPDATE":
				{
					// 通过Redo日志判断更新的字段是否为用户关心的
					Map<String, String> valMap = dmlBean.getAfter();
					boolean careFieldNotChange = true;
					for (String changedColumn : valMap.keySet())
					{
						if (this.tableSchema.getRowColumnNames().contains(changedColumn))
						{
							// 用户关心的字段发生了变化
							careFieldNotChange = false;
							break;
						}
					}
					if (careFieldNotChange)
					{
						// 没有变化就不触发更新了
						break;
					}
					// 通过解析Redo日志得到的Key查询更新后的数据
					Map<String, String> keyMap = dmlBean.getKeys();
					// 构造before的rowdata数据
					Row rowBefore = Row.withNames(RowKind.UPDATE_BEFORE);
					for (String key : keyMap.keySet())
					{
						rowBefore.setField(key, keyMap.get(key));
					}
					collector.collect((T) (rowClass == Row.class ? rowBefore : this.tableSchema.convertRowToRowData(rowBefore)));
					// 按照主键执行查询获得变更后记录所有字段的数据
					StringBuilder whereBuilder = new StringBuilder();
					whereBuilder.append(" WHERE ");
					Iterator<String> keyIt = keyMap.keySet().iterator();
					while (keyIt.hasNext())
					{
						String keyName = keyIt.next();
						whereBuilder.append(keyName);
						whereBuilder.append("='" + keyMap.get(keyName) + "' ");
						if (keyIt.hasNext())
						{
							whereBuilder.append("AND");
						}
					}
					Statement statement = this.dmDataBaseConnection.createStatement();
					ResultSet resultSet = statement.executeQuery(this.makeFullSelectSQL() + whereBuilder.toString());
					if (resultSet.next())
					{
						// 构造after字段
						Row row = Row.withNames(RowKind.UPDATE_AFTER);
						for (String columnName : this.tableSchema.getRowColumnNames())
						{
							DataType columnType = this.tableSchema.getDataTypeOfColumn(columnName);
							Object value = CabbageUtils.mapCDCValueToRowValue(columnType, resultSet.getObject(columnName), columnName);
							row.setField(columnName, value);
						}
						collector.collect((T) (rowClass == Row.class ? row : this.tableSchema.convertRowToRowData(row)));
					}
					resultSet.close();
					statement.close();
					break;
				}
				case "DELETE":
				{
					Map<String, String> keyMap = dmlBean.getKeys();
					Row row = Row.withNames(RowKind.DELETE);
					for (String columnName : keyMap.keySet())
					{
						DataType columnType = this.tableSchema.getDataTypeOfColumn(columnName);
						Object columnValue = CabbageUtils.mapCDCValueToRowValue(columnType, keyMap.get(columnName), columnName);
						row.setField(columnName, columnValue);
					}
					collector.collect((T) (rowClass == Row.class ? row : this.tableSchema.convertRowToRowData(row)));
					break;
				}
			}
			// 更新LSN到记录的COMMIT_SCN
			long newLSN = redoLog.getLong(2);
			this.dmIncrementLSN.clear();
			this.dmIncrementLSN.add(newLSN);
		}
		// 最后不要忘记关闭Redo分析
		this.dmQuerySelector.execute("DBMS_LOGMNR.END_LOGMNR()");
	}

	@Override
	public void cancel()
	{
		this.cancel = true;
		try
		{
			this.dmQuerySelector.close();
			this.dmDataBaseConnection.close();
		} catch (Exception e)
		{
			// 空指针和异常关闭的情况
		}
	}
}
