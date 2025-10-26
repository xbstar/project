package com.xbstar.chenguang;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.xbstar.FlinkTableRow;
import com.xbstar.FlinkTableSchema;
import com.xbstar.CabbageUtils;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** @author 陈光
 * 自定义以一个Debsume内部用于解析捕获到的数据的解析器，全增量都通过此解析器解析
 * 泛型是因为TableAPI要RowData而StreamAPI要Row，Flink也挺烦的搞这么多啰嗦
 *
 * */
public class MySQLSourceDebezium<T> implements DebeziumDeserializationSchema<T>
{
	/** 可以序列化的算子参数*/
	private final Class<T> recordClass; //TableAPI初始化的转RowData而StreamAPI要Row,如果是多表算子则用自定义的FlinkTableRow
	private final String jdbcUrl;
	private final String userName;
	private final String passWord;
	private final String dataBase;
	private final String tableName; //StreamAPI单表模式会给一个TableName
	private final FlinkTableSchema catalogSchema; //TableAPI会给一个生命的Catlog的Schema，需要进行比对
	private Map<String, FlinkTableSchema> jdbcSchemaMap = new TreeMap<>(); //缓存通过JDBC捕获的数据库的Schema
	/** 算子运行时生成的临时变量*/
	private transient boolean firstWholeInvoked = false;
	private transient boolean firstIncrementInvoked = false;
	private transient Set<String> currentTableSet;
	private transient Connection currentJDBCConnection;

	/** StreamAPI单表模式*/
	MySQLSourceDebezium(String host, int port, String dataBase, String userName, String passWord, String tableName)
	{
		this.tableName = tableName;
		this.jdbcUrl = "jdbc:mysql://" + host + ":" + port + "/" + dataBase;
		this.dataBase = dataBase;
		this.userName = userName;
		this.passWord = passWord;
		this.catalogSchema = null;
		this.recordClass = (Class<T>) Row.class;
	}

	/** StreamAPI多表模式*/
	MySQLSourceDebezium(String host, int port, String dataBase, String userName, String passWord)
	{
		this.tableName = null;
		this.jdbcUrl = "jdbc:mysql://" + host + ":" + port + "/" + dataBase;
		this.dataBase = dataBase;
		this.userName = userName;
		this.passWord = passWord;
		this.catalogSchema = null;
		this.recordClass = (Class<T>) FlinkTableRow.class;
	}

	/** TableAPI单表模式*/
	MySQLSourceDebezium(String host, int port, String dataBase, String userName, String passWord, FlinkTableSchema schema)
	{
		this.tableName = schema.getTableName();
		this.jdbcUrl = "jdbc:mysql://" + host + ":" + port + "/" + dataBase;
		this.dataBase = dataBase;
		this.userName = userName;
		this.passWord = passWord;
		this.catalogSchema = schema;
		this.recordClass = (Class<T>) RowData.class;
	}

	@Override
	public TypeInformation<T> getProducedType()
	{
		return BasicTypeInfo.of(this.recordClass);
	}

	public void open() throws Exception
	{
		// 首先获得数据库连接
		Class.forName("com.mysql.cj.jdbc.Driver");
		DriverManager.setLoginTimeout(10);
		this.currentJDBCConnection = DriverManager.getConnection(this.jdbcUrl, this.userName, this.passWord);
		this.currentJDBCConnection.setAutoCommit(true);// 关闭事务提交，后面试验下使用事务看速度是否提升
		// StreamAPI单表模式和TableApI模式下，需要检查目标表是否存在
		this.currentTableSet = CabbageUtils.getTableSetFromMySQL(currentJDBCConnection, dataBase);
		if ((this.recordClass == RowData.class || this.recordClass == Row.class) && !currentTableSet.contains(tableName))
		{
			throw new RuntimeException("FlinkCDCMySQLSource检测到指定的数据库" + dataBase + "不存在名为" + tableName + "的表！");
		}
		// TableApI下要进行Schema的比对
		if (this.recordClass == RowData.class)
		{
			// 通过JDBC获得表数据结构
			FlinkTableSchema jdbcSchema = CabbageUtils.getTableSchemaFormMySQL(currentJDBCConnection, tableName);
			this.jdbcSchemaMap.put(tableName, jdbcSchema); //缓存一下获得的schema数据结构
			/*Catlog中声明的字段必须是数据库真实字段的子集,另外因为大小写不敏感，统一转化为大写进行比较*/
			Set<String> jdbcColumnNames = jdbcSchema.getRowColumnNames().stream().map(name -> name.toUpperCase()).collect(Collectors.toSet());
			for (String declareColumn : catalogSchema.getRowColumnNames())
			{
				if (!jdbcColumnNames.contains(declareColumn.toUpperCase()))
				{
					System.out.println("\033[1;31m" + "FlinkCDCMySQLSource检测到" + catalogSchema.getTableName() + " 中声明的字段 " + declareColumn + " 在数据库表 " + tableName + " 中不存在" + "\033[0m");
					throw new IllegalArgumentException(catalogSchema.getTableName() + " 中声明的字段 " + declareColumn + " 在数据库表 " + tableName + " 中不存在");
				}
			}
		}
	}

	@Override
	public void deserialize(SourceRecord sourceRecord, Collector<T> collector) throws Exception
	{
		// 首次调用执行open方法
		if (this.firstIncrementInvoked == false && this.firstWholeInvoked == false)
		{
			this.open();
		}
		//获取库名&表名和时间戳
		Struct value = (Struct) sourceRecord.value();
		Struct source = value.getStruct("source");
		String database = source.getString("db");
		String tableName = source.getString("table");
		//获取操作类型
		Envelope.Operation operation = Envelope.operationFor(sourceRecord);
		if (operation == null)
		{
			//更新Schema的处理暂时不考虑
			return;
		}
		// 构造Row数据，首先获得用户定义和关心的列Schema
		if (operation.name().equals("READ"))
		{
			if (!this.firstWholeInvoked)
			{
				System.out.println("\033[1;36m" + "FlinkCDCMySQLSource_" + this.hashCode() + CabbageUtils.getCurrentTimeString() + "开始执行全量数据捕获" + "\033[0m");
				this.firstWholeInvoked = true;
			}
			Struct data = value.getStruct("after");
			FlinkTableRow row = this.parseFlinkTableRow(data, tableName, RowKind.INSERT);
			this.collectTableRow(collector, row);
		} else
		{
			if (!this.firstIncrementInvoked)
			{
				System.out.println("\033[1;36m" + "FlinkCDCMySQLSource_" + this.hashCode() + CabbageUtils.getCurrentTimeString() + "开始执行增量数据捕获" + "\033[0m");
				this.firstWholeInvoked = true;
			}
			switch (operation.name())
			{
				case "CREATE":
				{
					Struct data = value.getStruct("after");
					FlinkTableRow row = this.parseFlinkTableRow(data, tableName, RowKind.INSERT);
					this.collectTableRow(collector, row);
					break;
				}
				case "DELETE":
				{
					Struct data = value.getStruct("before");
					FlinkTableRow row = this.parseFlinkTableRow(data, tableName, RowKind.DELETE);
					this.collectTableRow(collector, row);
					break;
				}
				case "UPDATE":
				{
					Struct before = value.getStruct("before");
					Struct after = value.getStruct("after");
					// 比对用户关心的字段是否发生变化
					boolean updateFlag = false;
					if (this.catalogSchema == null)
					{
						updateFlag = true;
					} else
					{
						// 只有用户声明的字段发生变动，才是更新了
						for (String columnName : this.catalogSchema.getRowColumnNames())
						{
							Object beforeValue = before.get(columnName) == null ? "null" : before.get(columnName);
							Object afterValue = after.get(columnName) == null ? "null" : after.get(columnName);
							if (!beforeValue.equals(afterValue))
							{
								updateFlag = true;
								break;
							}
						}
					}
					// 只有用户关心的字段发生变化才会触发更新
					if (updateFlag)
					{
						FlinkTableRow rowBefore = this.parseFlinkTableRow(before, tableName, RowKind.UPDATE_BEFORE);
						FlinkTableRow rowAfter = this.parseFlinkTableRow(after, tableName, RowKind.UPDATE_AFTER);
						this.collectTableRow(collector, rowBefore);
						this.collectTableRow(collector, rowAfter);
					}
					break;
				}
			}
		}
	}

	/** 解析一条记录的数据，构造FlinkRow数据记录*/
	private FlinkTableRow parseFlinkTableRow(Struct dataSource, String tableName, RowKind rowKind) throws SQLException
	{
		// 优先用Catlog中定义的Schema,如果没有则上面应该保存到schemaMap中了
		FlinkTableSchema schema = this.catalogSchema != null ? this.catalogSchema : this.jdbcSchemaMap.get(tableName);
		if (schema == null)
		{
			schema = CabbageUtils.getTableSchemaFormMySQL(currentJDBCConnection, tableName);
			this.jdbcSchemaMap.put(tableName, schema);
		}
		// 开始构造row数据
		FlinkTableRow row = new FlinkTableRow(schema, rowKind);
		for (String columnName : schema.getRowColumnNames())
		{
			DataType type = schema.getDataTypeOfColumn(columnName);
			Object columnValue = CabbageUtils.mapCDCValueToRowValue(type, dataSource.get(columnName), columnName);
			row.setRowValueOfColumn(columnName, columnValue);
		}
		return row;
	}

	private void collectTableRow(Collector<T> collector, FlinkTableRow row)
	{
		// 根据类型返回构造的结果
		switch (recordClass.getSimpleName())
		{
			case "FlinkTableRow":
			{
				collector.collect((T) (row));
				break;
			}
			case "Row":
			{
				collector.collect((T) (row.convertToRow()));
				break;
			}
			case "RowData":
			{
				collector.collect((T) (row.convertToRowData()));
				break;
			}
		}
	}
}
