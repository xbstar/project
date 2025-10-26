package com.xbstar.chenguang;

import com.xbstar.FlinkTableRow;
import com.xbstar.FlinkTableSchema;
import com.xbstar.CabbageUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MySQLSinkFunction<T> extends RichSinkFunction<T> implements CheckpointedFunction
{
	/** 初始化算子基础数据*/
	private final Class<T> recordClass; //TableAPI初始化的转RowData而StreamAPI要Row,如果是多表算子则用自定义的FlinkTableRow
	private final String jdbcUrl;
	private final String userName;
	private final String passWord;
	private final String dataBase;
	private final String tableName;
	private final FlinkTableSchema catalogSchema; //FlinkRow数据结构从FlinkDynamicTable的Schema中获取
	/** 算子运行参数配置*/
	public static final int MaxBatchCount = 10000;
	/** 算子运行时生成的临时变量*/
	private static Map<String, FlinkTableSchema> jdbcSchemaMap = new HashMap<>(); //缓存多表模式下的所有已经获取到的数据库Schema
	private static Set<String> currentTableSet; //通过JDBC获取的当前数据库中的所有的表
	private transient boolean firstInvoke = false;
	private transient Connection currentJDBCConnection;
	private transient int currentBatchCount;
	private transient Statement currentStatement;

	/** StreamAPI单表模式*/
	MySQLSinkFunction(String host, int port, String dataBase, String userName, String passWord, String tableName)
	{
		this.jdbcUrl = "jdbc:mysql://" + host + ":" + port + "/" + dataBase;
		this.dataBase = dataBase;
		this.userName = userName;
		this.passWord = passWord;
		this.tableName = tableName;
		this.catalogSchema = null;
		this.recordClass = (Class<T>) Row.class;
	}

	/** StreamAPI多表模式*/
	MySQLSinkFunction(String host, int port, String dataBase, String userName, String passWord)
	{
		this.jdbcUrl = "jdbc:mysql://" + host + ":" + port + "/" + dataBase;
		this.dataBase = dataBase;
		this.userName = userName;
		this.passWord = passWord;
		this.tableName = null;
		this.catalogSchema = null;
		this.recordClass = (Class<T>) FlinkTableRow.class;
	}

	/** TableAPI单表模式*/
	MySQLSinkFunction(String host, int port, String dataBase, String userName, String passWord, FlinkTableSchema schema)
	{
		this.jdbcUrl = "jdbc:mysql://" + host + ":" + port + "/" + dataBase;
		this.dataBase = dataBase;
		this.userName = userName;
		this.passWord = passWord;
		this.tableName = schema.getTableName();
		this.catalogSchema = schema;
		this.recordClass = (Class<T>) RowData.class;
	}

	@Override
	public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception
	{
	}

	@Override
	public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception
	{
		System.out.println("\033[1;36m" + "MySQLSinkFunction_" + hashCode() + CabbageUtils.getCurrentTimeString() + "由于检查点" + functionSnapshotContext.getCheckpointId() + "保存，执行了BatchCommit事务提交了" + this.currentBatchCount + "条数据！！" + "\033[0m");
		this.currentJDBCConnection.commit();
		this.currentBatchCount = 0;
	}

	@Override  /*当算子打开的时候回调此接口*/
	public void open(Configuration parameters) throws Exception
	{
		// 首先获得数据库连接
		Class.forName("com.mysql.cj.jdbc.Driver");
		DriverManager.setLoginTimeout(10);
		this.currentJDBCConnection = DriverManager.getConnection(this.jdbcUrl, this.userName, this.passWord);
		this.currentBatchCount = 0;
		/** 设置是否开启事务，经过试验开启事务有重大的性能提升
		 *  试验目标：employee表，数据量：17065条
		 *  不开启事务：03:01:07 到 03:15:09 ->用时 14:02
		 *  开启事务：  03:28:51 到 03:35:34 ->用时  6:43
		 */
		this.currentJDBCConnection.setAutoCommit(false);// 开启事务
		this.currentStatement = this.currentJDBCConnection.createStatement();
		// StreamAPI单表模式和TableApI模式下，需要检查目标表是否存在
		if (currentTableSet == null)
		{
			currentTableSet = CabbageUtils.getTableSetFromMySQL(currentJDBCConnection, dataBase);
		}
		if ((this.recordClass == RowData.class || this.recordClass == Row.class) && !currentTableSet.contains(tableName))
		{
			throw new RuntimeException("MySQLSinkFunction检测到指定的数据库" + dataBase + "不存在名为" + tableName + "的表！");
		}
		// TableApI下要进行Schema的比对
		if (this.recordClass == RowData.class)
		{
			// 这种情况必为单表模式的TableAPI，需要与JDBC中的字段进行比对
			FlinkTableSchema jdbcSchema = CabbageUtils.getTableSchemaFormMySQL(currentJDBCConnection, catalogSchema.getTableName());
			this.jdbcSchemaMap.put(jdbcSchema.getTableName(), jdbcSchema);
			/*字段上声明的字段必须是数据库字段的子集
			 * 另外因为大小写不敏感，统一转化为大写进行比较
			 */
			for (String declareColumn : catalogSchema.getRowColumnNames())
			{
				if (!jdbcSchema.getRowColumnNames().contains(declareColumn.toUpperCase()))
				{
					System.out.println("\033[1;31m" + "MySQLSinkFunction检测到" + this.catalogSchema
							.getTableName() + " 中声明的字段 " + declareColumn + " 在数据库表 " + jdbcSchema.getTableName() + " 中不存在" + "\033[0m");
					throw new IllegalArgumentException(this.catalogSchema
							.getTableName() + " 中声明的字段 " + declareColumn + " 在数据库表 " + jdbcSchema.getTableName() + " 中不存在");
				}
			}
			/* 在主键的约束方面，数据库中的主键都必须在用户声明中匹配到
			 * 但是反过来是允许的，用户声明的主键不一定要在数据库中
			 * 可能数据没有声明主键，但是DynamicTable中声明了，更新按照声明的主键来匹配记录
			 */
			for (String jdbcPK : jdbcSchema.getPrimaryKeyNames())
			{
				if (!catalogSchema.getPrimaryKeyNames().contains(jdbcPK.toUpperCase()))
				{
					System.out.println("\033[1;31m" + "MySQLSinkFunction检测到数据库表 " + jdbcSchema.getTableName() + " 中的主键 " + jdbcPK + " 未在" + this.catalogSchema
							.getTableName() + "中声明" + "\033[0m");
					throw new IllegalArgumentException("数据库表 " + jdbcSchema.getTableName() + " 中的主键 " + jdbcPK + " 未在" + this.catalogSchema.getTableName() + "中声明");
				}
			}
			/* JDBC中的非空字段都必须在用户字段中声明*/
			for (String jdbcNotNull : jdbcSchema.getNotNullColumnNames())
			{
				if (!catalogSchema.getRowColumnNames().contains(jdbcNotNull.toUpperCase()))
				{
					System.out.println("\033[1;31m" + "MySQLSinkFunction检测到数据库表 " + jdbcSchema.getTableName() + " 中的非空字段 " + jdbcNotNull + " 必须在" + this.catalogSchema
							.getTableName() + "中声明" + "\033[0m");
					throw new IllegalArgumentException("数据库表 " + jdbcSchema.getTableName() + " 中的非空字段 " + jdbcNotNull + " 必须在" + this.catalogSchema
							.getTableName() + "中声明");
				}
			}
		}
	}

	@Override /* 算子开始工作时回调此接口*/
	public void invoke(T value, Context context) throws Exception
	{
		if (!this.firstInvoke)
		{
			System.out.println("\033[1;36m" + "MySQLSinkFunciton_" + this.hashCode() + CabbageUtils.getCurrentTimeString() + "开始执行数据库数据插入" + "\033[0m");
			this.firstInvoke = true;
		}
		// 统一转化为FlinkTableRow，注意在单表StreamAPI模式下丢失了所有字段的类型信息
		FlinkTableRow row = null;
		if (value instanceof RowData)
		{
			// TableAPI模式必有catalogSchema
			row = FlinkTableRow.createFormRowData(this.catalogSchema, (RowData) value);
		} else if (value instanceof Row)
		{
			// StreamApi单表模式必有tableName
			row = FlinkTableRow.createFromRow(this.tableName, (Row) value);
		} else if (value instanceof FlinkTableRow)
		{
			// StreamAPI多表模式下直接就是所求
			row = (FlinkTableRow) value;
		} else
		{
			// 没有其他情况了，报错
			System.out.println("\033[1;31m" + "MySQLSinkFunction检测到流数据类型必须是" + recordClass.getSimpleName() + ",当前传入的是:" + value.getClass().getSimpleName() + "\033[0m");
			throw new IllegalAccessException("流数据类型必须是" + recordClass.getSimpleName() + ",当前传入的是:" + value.getClass().getSimpleName());
		}
		// 如果目标数据库还没有这张表，则创建
		synchronized (currentTableSet)
		{
			if (!currentTableSet.contains(row.getTableName()))
			{
				row.getSchema().createTableInDataBase(this.currentJDBCConnection);
				System.out.println("\033[1;36m" + "MySQLSinkFunction在" + dataBase + "中创建了数据表" + row.getSchema().getTableName() + "\033[0m");
				currentTableSet.add(row.getTableName());
				jdbcSchemaMap.put(row.getTableName(), row.getSchema());
			}
		}
		// 获取数据库中实际表的Schema，尝试将流数据转为符合JDBCSchema定义的数据
		FlinkTableSchema jdbcSchema;
		synchronized (jdbcSchemaMap)
		{
			jdbcSchema = jdbcSchemaMap.get(row.getTableName());
			if (jdbcSchema == null)
			{
				System.out.println("\033[1;36m" + "MySQLSinkFunction从JDBC中读取了数据库" + dataBase + "中表" + row.getTableName() + "的元数据信息" + "\033[0m");
				jdbcSchema = CabbageUtils.getTableSchemaFormMySQL(currentJDBCConnection, row.getTableName());
				jdbcSchemaMap.put(row.getTableName(), jdbcSchema);
			}
		}
		// 尝试将流数据转化为标准可插入数据库数据，转化不了则报错
		FlinkTableRow record = FlinkTableRow.createWithNewSchema(jdbcSchema, row);
		// 开始执行插入
		switch (record.getRowKind())
		{
			case INSERT:
			case UPDATE_AFTER:
			{
				CabbageUtils.insertRowToMySQL(this.currentJDBCConnection, record);
				this.currentBatchCount++;
				if (this.currentBatchCount >= MaxBatchCount)
				{
					System.out.println("\033[1;36m" + "MySQLSinkFunction_" + hashCode() + CabbageUtils.getCurrentTimeString() + "由于事务数据达到了" + currentBatchCount + "条，执行了BatchCommit事务提交！！" + "\033[0m");
					this.currentJDBCConnection.commit();
					this.currentBatchCount = 0;
				}
				break;
			}
			case DELETE:
			{
				CabbageUtils.deleteRowFromMySQL(this.currentJDBCConnection, record);
				break;
			}
		}
	}

	@Override /* 算子关闭时回调*/
	public void close() throws Exception
	{
		// 即使invoke抛出异常，close也一定会调用
		currentStatement.executeBatch();
		if (!currentJDBCConnection.getAutoCommit()) currentJDBCConnection.commit(); //把所有的事务都提交
		currentStatement.close();
		currentJDBCConnection.close();
	}
}
