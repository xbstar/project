package com.xbstar;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.*;
import java.util.stream.Collectors;

public class CabbageUtils
{
	/** 获取当前的时间字符串*/
	public static String getCurrentTimeString()
	{
		return "@" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
	}

	/** 拷贝源库的所有表，并在每个表中拷贝5条数据*/
	public static void copyDateBaseSchema(Connection from, Connection to) throws SQLException
	{
		Statement fromStatement = from.createStatement();
		// 获取数据源库的数据库名称
		ResultSet resultSet = fromStatement.executeQuery("SELECT DATABASE()");
		if (!resultSet.next()) throw new RuntimeException("不能获得源数据库名");
		String fromDatabase = resultSet.getString(1);
		resultSet.close();
		// 获取目标库数据库名称
		Statement toStatement = to.createStatement();
		resultSet = toStatement.executeQuery("SELECT DATABASE()");
		if (!resultSet.next()) throw new RuntimeException("不能获得目标库名称");
		String toDataBase = resultSet.getString(1);
		resultSet.close();
		// 打印数据库结果
		System.out.println("From:" + fromDatabase);
		System.out.println("To:" + toDataBase);
		// 获取源数据库下所有的表
		DatabaseMetaData metaData = from.getMetaData();
		ResultSet tables = metaData.getTables(fromDatabase, null, "%", new String[] { "TABLE" });
		// 获取目标库表名集合
		Set<String> targetSet = CabbageUtils.getTableSetFromMySQL(to, toDataBase);
		// 设置不用事务，忽略外键检查，忽略零值插入
		to.setAutoCommit(true);
		toStatement.execute("SET FOREIGN_KEY_CHECKS = 0");
		toStatement.execute("SET SESSION SQL_MODE=NO_ENGINE_SUBSTITUTION");
		// 逐张表进行处理
		while (tables.next())
		{
			// 首先从源库中获取建表语句
			String tableName = tables.getString("TABLE_NAME");
			//			String tableName = "baohu_qiju_info";
			System.out.println("\033[1;36m" + "处理数据表" + "\033[0m" + tableName);
			ResultSet ddlCursor = fromStatement.executeQuery("show create table `" + tableName+"`");
			ddlCursor.next();
			String ddl = ddlCursor.getString("Create Table");
			ddlCursor.close();
			// 并在目标库中创建表创建表
			if (!targetSet.contains(tableName))
			{
				toStatement.execute(ddl);
				targetSet.add(tableName);
			}
			// 然后拷贝5条数据
			ResultSet dataSet = fromStatement.executeQuery("select * from `" + tableName + "` limit 5");
			int insertCount = 0;
			while (dataSet.next())
			{
				String insertSQL = "replace into `" + tableName + "` values(";
				for (int j = 0; j < dataSet.getMetaData().getColumnCount(); j++)
				{
					insertSQL += "?,";
				}
				insertSQL = insertSQL.substring(0, insertSQL.length() - 1) + ")";
				// 执行一条数据插入
				PreparedStatement ps = to.prepareStatement(insertSQL);
				boolean error = false;
				String record = "(";
				for (int j = 1; j <= dataSet.getMetaData().getColumnCount(); j++)
				{
					Object value = null;
					try
					{
						value = dataSet.getObject(j);

					} catch (Exception e)
					{
						error = true;
						value = dataSet.getString(j);
					}
					record += value + ",";
					ps.setObject(j, value);
				}
				record = record.substring(0, record.length() - 1) + ")";
				if (error) System.out.println("\033[1;31m" + "发生了数据错误" + record.toString() + "\033[0m");
				ps.executeUpdate();
				ps.close();
				if (++insertCount > 5) break;
			}
		}
		fromStatement.close();
		toStatement.close();
	}

	/**
	 * 将从CDC框架中或者其他框架获取到的数据转化为Flink需要的行数据的方法
	 * @param type Flink 数据类型
	 * @param cdcValue CDC捕获到的数据
	 * @param columnName 列字段的名称只是为了报错提示，可以不传
	 * @return 转化后符合要求的RowData数据
	 */
	public static Object mapCDCValueToRowValue(DataType type, Object cdcValue, String columnName)
	{
		if (cdcValue == null)
		{
			// 为null不需要转化，直接返回
			return null;
		}
		switch (type.getLogicalType().getTypeRoot())
		{
			case BOOLEAN:
			{
				/** Debezium捕获到的是Boolean，转成Boolean所以不用处理*/
				return cdcValue;
			}
			case BINARY:
			{
				if (cdcValue instanceof ByteBuffer)
				{
					/**
					 * MySQL数据类型为binary时Debezium捕获到的是java.nio.HeapByteBuffer
					 * 代表的是二进制的字节序列，一般用16进制表示
					 * 我们转化为字符串，形如Ox310000，这样sink算子可以直接插入
					 */
					ByteBuffer buffer = (ByteBuffer) cdcValue;
					byte[] bytes = buffer.array();
					StringBuilder hexString = new StringBuilder();
					hexString.append("0x");
					for (byte b : bytes)
					{
						hexString.append(String.format("%02X", b));
					}
					return hexString.toString();
				} else
				{
					/**
					 * MySQL数据类型为bit且length大于1的时候，Debezium捕获到的是字节数组byte[]
					 * 其中cdcValue[0]是这个二进制数值对应的整形值
					 * 其中cdcValue[1]一直是0不知道有什么用
					 * 我们转化为字符串，形如B'01010101'，这样sink算子可以直接插入
					 */
					byte[] bytes = (byte[]) cdcValue;
					BinaryType cast = (BinaryType) type.getLogicalType();
					return String.format("B'%" + cast.getLength() + "s'", Integer.toBinaryString((byte) bytes[0])).replace(' ', '0');
				}

			}
			case VARBINARY:
			{
				/** 他们数据都是null，所以暂时不处理了*/
				return cdcValue.toString();
			}
			case INTEGER:
			{
				/** Debezium捕获到的是Integer，转成Integer所以不用处理*/
				return cdcValue;
			}
			case TINYINT:
			case SMALLINT:
			{
				/** Debezium捕获到的是Short，转成Short所以不用处理*/
				return cdcValue;
			}
			case BIGINT:
			{
				/** Debezium捕获到的是Long，转成Long所以不用处理*/
				return cdcValue;
			}
			case FLOAT:
			case DOUBLE:
			{
				/** Debezium捕获到的是Double，转成Double所以不用处理*/
				return cdcValue;
			}
			case DECIMAL:
			{
				/**
				 * MySQL数据类型为decimal时Debezium捕获到的是BigDecimal
				 * 其中precision代表总位数（包含小数）
				 * 其中scale代表小数位数，比如decimal(3,2)则只能保存形如5.23的浮点数，整数位不能超过10，小数位不能超过3为
				 * 整数位超出范围是会报错插不进去报out of range异常，小数位超出能插进去后面的位数会被直接丢弃
				 */
				BigDecimal decValue = (BigDecimal) cdcValue;
				DecimalType decType = (DecimalType) type.getLogicalType();
				return DecimalData.fromBigDecimal(decValue, decType.getPrecision(), decType.getScale());
			}
			case CHAR:
			case VARCHAR:
			{
				/** Debezium捕获到的是String，转成String所以不用处理*/
				return cdcValue.toString();
			}
			case DATE:
			{
				//				System.out.println(cdcValue.getClass());
				//				System.out.println(cdcValue);
				/**
				 * MySQL数据类型为date时Debezium捕获到的是int
				 * 意义是从格林尼治时间开始的天数
				 * 需要转化为形如：2024-01-03这样的字符串
				 */
				Instant instant = Instant.ofEpochSecond((int) cdcValue * 24 * 60 * 60);
				LocalDate localDate = instant.atOffset(ZoneOffset.UTC).toLocalDate();
				return localDate.toString();
			}
			case TIME_WITHOUT_TIME_ZONE:
			{
				/**
				 * MySQL数据类型为time时Debezium捕获到的是Long
				 * 意义是分秒（就是1毫秒的1000分之一）要换算成对应的时间
				 * 最终转化为形如15:30:00的字符串
				 */
				long milliseconds = (Long) cdcValue / 1000;
				long totalSeconds = milliseconds / 1000;
				long hours = totalSeconds / 3600;
				long remainingSeconds = totalSeconds % 3600;
				long minutes = remainingSeconds / 60;
				long seconds = remainingSeconds % 60;
				return String.format("%02d", hours) + ":" + String.format("%02d", minutes) + ":" + String.format("%02d", seconds);
			}
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			{
				if (cdcValue instanceof String)
				{
					/**
					 * MySQL数据类型为timestamp时Debezium捕获到的是String，形如2022-12-15T16:58:46Z
					 * 意义是符合 ISO 8601 标准的时间格式，也被称为 UTC 时间格式，其中Z: 表示 UTC（协调世界时）时间，即零时区
					 * 需要转化为形如2023-12-26 15:30:00的字符串
					 */
					Instant instant = Instant.parse((String) cdcValue);
					DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
					return instant.atZone(ZoneId.of("Asia/Shanghai")).format(formatter);
				} else
				{
					/**
					 * MySQL数据类型为datetime时Debezium捕获到的是Long
					 * 意义是以UTC为时区的毫秒值
					 * 需要转化为形如2023-12-26 15:30:00的字符串
					 */
					DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					format.setTimeZone(TimeZone.getTimeZone("UTC"));
					return format.format(new Date((Long) cdcValue));
				}
			}
			default:
			{
				throw new IllegalArgumentException("不支持的数据类型的列：" + columnName);
			}
		}
	}

	/**
	 * 从MySQL的JDBC链接中获得所有数据表的集合
	 * @param jdbcConnection 传入JDBC链接
	 * @param dataBase 数据库名
	 * @return 所有表名的集合
	 */
	public static Set<String> getTableSetFromMySQL(Connection jdbcConnection, String dataBase) throws SQLException
	{
		String sql = "SELECT "
				+ "TABLE_NAME "
				+ "FROM "
				+ "INFORMATION_SCHEMA.TABLES "
				+ "WHERE "
				+ "TABLE_SCHEMA = '" + dataBase + "'";
		ResultSet tableCursor = jdbcConnection.createStatement().executeQuery(sql);
		Set<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
		while (tableCursor.next())
		{
			String tableName = tableCursor.getString("TABLE_NAME");
			result.add(tableName);
		}
		return result;
	}

	/**
	 * 从MYSQL的JDBC链接中获得数据表的字段信息
	 * @param jdbcConnection 传入JDBC链接
	 * @param tableName  表名
	 * @return 数据字段Schema描述
	 * @throws SQLException 抛出异常
	 */
	public static FlinkTableSchema getTableSchemaFormMySQL(Connection jdbcConnection, String tableName) throws SQLException
	{
		// 执行JDBC查询获取字段
		String dataBase = jdbcConnection.getCatalog();
		String sql = "SELECT "
				+ "COLUMN_NAME,"
				+ "DATA_TYPE,"
				+ "CHARACTER_MAXIMUM_LENGTH,"
				+ "NUMERIC_PRECISION,"
				+ "NUMERIC_SCALE,"
				+ "DATETIME_PRECISION,"
				+ "COLUMN_KEY,"
				+ "IS_NULLABLE,"
				+ "COLUMN_DEFAULT,"
				+ "COLUMN_COMMENT "
				+ "FROM INFORMATION_SCHEMA.COLUMNS "
				+ "WHERE "
				+ "table_schema='" + dataBase + "' "
				+ "AND "
				+ "table_name='" + tableName + "';";
		Statement statement = jdbcConnection.createStatement();
		ResultSet columnCursor = statement.executeQuery(sql);
		if (!columnCursor.isBeforeFirst())
		{
			throw new RuntimeException("指定的数据库" + dataBase + "不存在或者表" + tableName + "不存在！");
		}
		List<DataTypes.Field> fieldList = new ArrayList<>();
		Set<String> pkSet = new HashSet<>();
		Set<String> notnullSet = new HashSet<>();
		while (columnCursor.next())
		{
			String columnName = columnCursor.getString("COLUMN_NAME");
			String columnType = columnCursor.getString("DATA_TYPE");
			// 不必担心大小写的问题，就算你用的大写的数据类型，MySQL也会自动转为小写，最终出现在column表里的都是小写
			switch (columnType.trim())
			{
				case "bit":
				{
					int precision = columnCursor.getInt("NUMERIC_PRECISION");
					if (precision == 1)
					{
						fieldList.add(DataTypes.FIELD(columnName, DataTypes.BOOLEAN()));
					} else
					{
						/** 这样做不仅对数据库没有什么影响，并且我们就可以通过precision是否能被8整除，来判断sink表创建的时候应该用bit还是binary*/
						if (precision % 8 == 0) precision += 1;
						fieldList.add(DataTypes.FIELD(columnName, DataTypes.BINARY(precision)));
					}
					break;
				}
				case "blob":
				{
					fieldList.add(DataTypes.FIELD(columnName, DataTypes.BYTES()));
					break;
				}
				case "binary":
				{
					int precision = columnCursor.getInt("CHARACTER_MAXIMUM_LENGTH");
					fieldList.add(DataTypes.FIELD(columnName, DataTypes.BINARY(precision * 8)));
					break;
				}
				case "year":
				case "int":
				case "mediumint":
				{
					fieldList.add(DataTypes.FIELD(columnName, DataTypes.INT()));
					break;
				}
				case "tinyint":
				{
					fieldList.add(DataTypes.FIELD(columnName, DataTypes.TINYINT()));
					break;
				}
				case "smallint":
				{
					fieldList.add(DataTypes.FIELD(columnName, DataTypes.SMALLINT()));
					break;
				}
				case "bigint":
				{
					fieldList.add(DataTypes.FIELD(columnName, DataTypes.BIGINT()));
					break;
				}
				case "float":
				{
					fieldList.add(DataTypes.FIELD(columnName, DataTypes.FLOAT()));
					break;
				}
				case "double":
				{
					fieldList.add(DataTypes.FIELD(columnName, DataTypes.DOUBLE()));
					break;
				}
				case "decimal":
				{
					int precision = columnCursor.getInt("NUMERIC_PRECISION");
					int scale = columnCursor.getInt("NUMERIC_SCALE");
					if (precision > 38) precision = 38;
					fieldList.add(DataTypes.FIELD(columnName, DataTypes.DECIMAL(precision, scale)));
					break;
				}
				case "char":
				{
					int dataLength = columnCursor.getInt("CHARACTER_MAXIMUM_LENGTH");
					fieldList.add(DataTypes.FIELD(columnName, DataTypes.CHAR(dataLength)));
					break;
				}
				case "varchar":
				{
					int dataLength = columnCursor.getInt("CHARACTER_MAXIMUM_LENGTH");
					if (dataLength >= 2147483647) dataLength = 2147483646;
					if (dataLength <= 0) dataLength = 1;
					fieldList.add(DataTypes.FIELD(columnName, DataTypes.VARCHAR(dataLength)));
					break;
				}
				case "text":
				case "tinytext":
				case "longtext":
				{
					fieldList.add(DataTypes.FIELD(columnName, DataTypes.STRING()));
					break;
				}
				case "time":
				{
					int precision = columnCursor.getInt("DATETIME_PRECISION");
					fieldList.add(DataTypes.FIELD(columnName, DataTypes.TIME(precision)));
					break;
				}
				case "date":
				{
					fieldList.add(DataTypes.FIELD(columnName, DataTypes.DATE()));
					break;
				}
				case "datetime":
				case "timestamp":
				{
					int precision = columnCursor.getInt("DATETIME_PRECISION");
					fieldList.add(DataTypes.FIELD(columnName, DataTypes.TIMESTAMP(precision)));
					break;
				}
				default:
				{
					throw new IllegalArgumentException(dataBase + " 数据库的 " + tableName + " 表的 " + columnName + " 字段，配置了不支持的数据类型 " + columnType);
				}
			}
			if ("PRI".equals(columnCursor.getString("COLUMN_KEY")))
			{
				pkSet.add(columnName);
			}
			if ("NO".equals(columnCursor.getString("IS_NULLABLE")))
			{
				notnullSet.add(columnName);
			}
		}
		columnCursor.close();
		statement.close();
		return new FlinkTableSchema(tableName, fieldList, pkSet, notnullSet);
	}

	/**
	 * 从达梦的JDBC链接中获得数据表的字段信息
	 * @param jdbcConnection 传入的达梦JDBC链接
	 * @param tableName 表名称
	 * @return 数据字段信息
	 * @throws SQLException 抛出异常
	 */
	public static FlinkTableSchema getTableSchemaFromDM(Connection jdbcConnection, String tableName) throws SQLException
	{
		// 用户没有传入Schema，从JDBC中读取表配置
		String dataBase = jdbcConnection.getCatalog();
		ResultSet columnCursor = jdbcConnection.createStatement().executeQuery("SELECT "
				+ "COLUMN_NAME,"
				+ "DATA_TYPE,"
				+ "DATA_LENGTH,"
				+ "DATA_PRECISION,"
				+ "DATA_SCALE,"
				+ "NULLABLE "
				+ "FROM ALL_TAB_COLUMNS "
				+ "WHERE "
				+ "OWNER='" + dataBase.toUpperCase() + "' "
				+ "AND "
				+ "TABLE_NAME='" + tableName.toUpperCase() + "'");
		if (!columnCursor.isBeforeFirst())
		{
			throw new RuntimeException("指定的模式" + dataBase + "不存在或者表" + tableName + "不存在！");
		}
		List<DataTypes.Field> fieldList = new ArrayList<>();
		Set<String> notnullSet = new HashSet<>();
		while (columnCursor.next())
		{
			String columnName = columnCursor.getString("COLUMN_NAME");
			String dataType = columnCursor.getString("DATA_TYPE");
			boolean nullAble = columnCursor.getString("NULLABLE").equals("Y") ? true : false;
			switch (dataType)
			{
				case "INT":
				{
					fieldList.add(DataTypes.FIELD(columnName, DataTypes.INT()));
					break;
				}
				case "DATE":
				{
					fieldList.add(DataTypes.FIELD(columnName, DataTypes.DATE()));
					break;
				}
				case "DECIMAL":
				{
					int precition = columnCursor.getInt("DATA_PRECISION");
					int scale = columnCursor.getInt("DATA_SCALE");
					fieldList.add(DataTypes.FIELD(columnName, DataTypes.DECIMAL(precition, scale)));
					break;
				}
				case "VARCHAR":
				{
					int dataLength = columnCursor.getInt("DATA_LENGTH");
					fieldList.add(DataTypes.FIELD(columnName, DataTypes.VARCHAR(dataLength)));
					break;
				}
				default:
				{
					throw new IllegalArgumentException(dataBase + "模式的" + tableName + "表的" + columnName + "字段，配置了不支持的数据类型" + dataType);
				}
			}
			// 非空键的初始化
			if ("N".equals(columnCursor.getString("NULLABLE").trim()))
			{
				notnullSet.add(columnName);
			}
		}
		columnCursor.close();
		// 进行主键的查询
		Set<String> pkSet = new HashSet<>();
		ResultSet pkCursor = jdbcConnection.createStatement().executeQuery(
				"SELECT COLUMN_NAME FROM ALL_CONSTRAINTS,ALL_CONS_COLUMNS "
						+ "WHERE "
						+ "ALL_CONSTRAINTS.CONSTRAINT_NAME=ALL_CONS_COLUMNS.CONSTRAINT_NAME "
						+ "AND "
						+ "ALL_CONSTRAINTS.CONSTRAINT_TYPE='P' "
						+ "AND "
						+ "ALL_CONS_COLUMNS.table_name='STUDENT'"
		);
		if (!pkCursor.isBeforeFirst())
		{
			throw new RuntimeException("指定的模式" + dataBase + "的" + tableName + "表必须包含至少一个主键！");
		}
		while (pkCursor.next())
		{
			pkSet.add(pkCursor.getString("COLUMN_NAME"));
		}
		pkCursor.close();
		return new FlinkTableSchema(tableName, fieldList, pkSet, notnullSet);
	}

	//	public static FlinkTableSchema getTableSchemaFromDebeziumSourceRecord(SourceRecord record)
	//	{
	//		// 构造所有的列
	//		List<DataTypes.Field> fieldList = new ArrayList<>();
	//		Struct value = (Struct) record.value();
	//		Struct source = value.getStruct("source");
	//		String database = source.getString("db");
	//		String tableName = source.getString("table");
	//		Struct data = value.getStruct("after");
	//		data = data == null ? value.getStruct("before") : data;
	//		for (Field field : data.schema().fields())
	//		{
	//			Schema schema = field.schema();
	//			//首先使用name来判断这是最准确的
	//			if (schema.name() == null)
	//			{
	//				// 如果name没有（可能是基础数据类型比如vachar或者int）那么只能依据基础类型判断
	//				switch (schema.type())
	//				{
	//					case STRING:
	//					{
	//						fieldList.add(DataTypes.FIELD(field.name(), DataTypes.STRING()));
	//						break;
	//					}
	//					case INT32:
	//					{
	//						fieldList.add(DataTypes.FIELD(field.name(), DataTypes.INT()));
	//						break;
	//					}
	//					case INT64:
	//					{
	//						fieldList.add(DataTypes.FIELD(field.name(), DataTypes.BIGINT()));
	//						break;
	//					}
	//					default:
	//					{
	//						throw new IllegalArgumentException("没有处理的类型:" + schema.type());
	//					}
	//				}
	//			}
	//			else
	//			{
	//				switch (schema.name())
	//				{
	//					case "io.debezium.time.Date":
	//					{
	//						fieldList.add(DataTypes.FIELD(field.name(), DataTypes.DATE()));
	//						break;
	//					}
	//					case "com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Decimal":
	//					{
	//						fieldList.add(DataTypes.FIELD(field.name(), DataTypes.DECIMAL(10, 2)));
	//						break;
	//					}
	//					case "io.debezium.data.Enum":
	//					{
	//						fieldList.add(DataTypes.FIELD(field.name(), DataTypes.STRING()));
	//						break;
	//					}
	//					case "io.debezium.time.MicroTime":
	//					{
	//						fieldList.add(DataTypes.FIELD(field.name(), DataTypes.TIME()));
	//						break;
	//					}
	//					case "io.debezium.time.Timestamp":
	//					{
	//						fieldList.add(DataTypes.FIELD(field.name(), DataTypes.TIMESTAMP()));
	//						break;
	//					}
	//					case "io.debezium.time.ZonedTimestamp":
	//					{
	//						fieldList.add(DataTypes.FIELD(field.name(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()));
	//						break;
	//					}
	//					default:
	//					{
	//						throw new IllegalArgumentException("未知的类型：" + schema.name());
	//					}
	//				}
	//			}
	//		}
	//		// 构造主键
	//		Set<String> pkSet = new HashSet<>();
	//		Struct key = (Struct) record.key();
	//		for (Field field : key.schema().fields())
	//		{
	//			pkSet.add(field.name());
	//		}
	//		// 构造非空键
	//		Set<String> notNullSet = new HashSet<>();
	//		for (Field field : data.schema().fields())
	//		{
	//			if (!field.schema().isOptional())
	//			{
	//				notNullSet.add(field.name());
	//			}
	//		}
	//		// 获得表名
	//		return new FlinkTableSchema(database, tableName, fieldList, pkSet, notNullSet);
	//	}

	/** 通过Connection连接到的数据库，自动根据tableSchema创建对应的表*/
	public static void createTableToMySQL(Connection connection, FlinkTableSchema schema)
	{
		StringBuilder sqlBuild = new StringBuilder();
		sqlBuild.append("CREATE TABLE ");
		sqlBuild.append("`" + schema.getTableName() + "`");
		sqlBuild.append(" (\n");
		Iterator<String> columnIt = schema.getRowColumnNames().iterator();
		while (columnIt.hasNext())
		{
			String column = columnIt.next();
			// 列名定义
			sqlBuild.append("`" + column + "`");
			// 类型定义
			DataType type = schema.getDataTypeOfColumn(column);
			switch (type.getLogicalType().getTypeRoot())
			{
				case BOOLEAN:
				{
					sqlBuild.append(" Bit(1) ");
					break;
				}
				case BINARY:
				{
					/** 在通过MySQLJDBC创建Schema的时候，我们已经对precision做了手脚
					 * 保障如果是通过Bit定义的字段，其precison必不能被8整除
					 * 这样同时解决了在mysql建表的时候，根据Flink的BINARY类型来确认应该用bit还是binary的问题
					 * 统一了BINARY类型的precision统一标识bit位
					 * 对数据库造成的影响是sink库的bit类型（如果正好能被8整除）可能多1位，这根本没什么影响
					 */
					BinaryType btype = (BinaryType) type.getLogicalType();
					int precision = btype.getLength();
					if (precision % 8 == 0)
					{
						sqlBuild.append(" BINARY(" + precision / 8 + ") ");
					} else
					{
						sqlBuild.append(" BIT(" + precision + ") ");
					}
					break;
				}
				case VARBINARY:
				{
					sqlBuild.append(" BLOB");
					break;
				}
				case INTEGER:
				{
					sqlBuild.append(" INTEGER ");
					break;
				}
				case SMALLINT:
				{
					sqlBuild.append(" SMALLINT ");
					break;
				}
				case TINYINT:
				{
					sqlBuild.append(" TINYINT ");
					break;
				}
				case BIGINT:
				{
					sqlBuild.append(" BIGINT ");
					break;
				}
				case FLOAT:
				{
					sqlBuild.append(" FLOAT ");
					break;
				}
				case DOUBLE:
				{
					sqlBuild.append(" DOUBLE ");
					break;
				}
				case DECIMAL:
				{
					DecimalType dtype = (DecimalType) type.getLogicalType();
					sqlBuild.append(" DECIMAL(" + dtype.getPrecision() + "," + dtype.getScale() + ") ");
					break;
				}
				case CHAR:
				{
					CharType ctype = (CharType) type.getLogicalType();
					sqlBuild.append(" CHAR(" + ctype.getLength() + ") ");
					break;
				}
				case VARCHAR:
				{
					if ("STRING".equals(type.toString()))
					{
						sqlBuild.append(" TEXT ");
					} else
					{
						VarCharType vchar = (VarCharType) type.getLogicalType();
						sqlBuild.append(" VARCHAR(" + vchar.getLength() + ") ");
					}
					break;
				}
				case DATE:
				{
					sqlBuild.append(" DATE ");
					break;
				}
				case TIME_WITHOUT_TIME_ZONE:
				{
					sqlBuild.append(" TIME ");
					break;
				}
				case TIMESTAMP_WITHOUT_TIME_ZONE:
				{
					sqlBuild.append(" DATETIME ");
					break;
				}
				default:
				{
					throw new RuntimeException(column + "列出现了你没有考虑到的列数据类型：" + type.getLogicalType().getTypeRoot());
				}
			}
			// 非空定义
			if (schema.getNotNullColumnNames().contains(column))
			{
				sqlBuild.append(" NOT NULL ");
			}
			// ,分隔
			if (columnIt.hasNext() || schema.getPrimaryKeyNames().size() > 0)
			{
				sqlBuild.append(",\n");
			}
		}
		// 主键定义
		if (schema.getPrimaryKeyNames().size() > 0)
		{
			sqlBuild.append("PRIMARY KEY (");
			Iterator<String> pkit = schema.getPrimaryKeyNames().iterator();
			while (pkit.hasNext())
			{
				sqlBuild.append("`" + pkit.next() + "`");
				if (pkit.hasNext())
				{
					sqlBuild.append(",");
				}
			}
		}
		sqlBuild.append(")\n)");
		// 执行建表语句
		try
		{
			Statement statement = connection.createStatement();
			statement.execute(sqlBuild.toString());
			statement.close();
		} catch (SQLException e)
		{
			System.out.println(sqlBuild.toString());
			throw new RuntimeException(e);
		}
	}

	/** 将记录插入到MySQL数据库中*/
	public static void insertRowToMySQL(Connection connection, FlinkTableRow record)
	{
		// 构造插入SQL语句
		StringBuilder sql = new StringBuilder();
		sql.append("REPLACE INTO ");
		sql.append("`" + record.getTableName() + "` (");
		Iterator<String> it = record.getSchema().getRowColumnNames().iterator();
		while (it.hasNext())
		{
			sql.append("`" + it.next() + "`");
			if (it.hasNext())
			{
				sql.append(",");
			}
		}
		sql.append(") VALUES (");
		it = record.getSchema().getRowColumnNames().iterator();
		while (it.hasNext())
		{
			String columnName = it.next();
			Object columnValue = record.getRowValueOfColumn(columnName);
			if (columnValue == null)
			{
				sql.append("null");
			} else
			{
				LogicalTypeRoot type = record.getSchema().getDataTypeOfColumn(columnName).getLogicalType().getTypeRoot();
				Set<LogicalTypeRoot> noQuotSet = Arrays.stream(new LogicalTypeRoot[] {
						LogicalTypeRoot.BINARY,
						LogicalTypeRoot.BOOLEAN,
						LogicalTypeRoot.INTEGER
				}).collect(Collectors.toSet());
				if (!noQuotSet.contains(type)) sql.append("'");
				if (columnValue instanceof String) columnValue = ((String) columnValue).replace("'", "''");
				sql.append(columnValue);
				if (!noQuotSet.contains(type)) sql.append("'");
			}
			if (it.hasNext())
			{
				sql.append(",");
			}
		}
		sql.append(")");
		// 执行插入更新SQL语句
		try
		{
			Statement statement = connection.createStatement();
			statement.execute(sql.toString());
			statement.close();
		} catch (Exception e)
		{
			System.out.println(sql);
			throw new RuntimeException(e);
		}
	}

	/** 从MySQL数据库中删除记录*/
	public static void deleteRowFromMySQL(Connection connection, FlinkTableRow record)
	{
		StringBuilder sql = new StringBuilder();
		sql.append("DELETE FROM ");
		sql.append(record.getTableName());
		sql.append(" WHERE ");
		Iterator<String> it = record.getSchema().getPrimaryKeyNames().iterator();
		while (it.hasNext())
		{
			String filedName = it.next();
			sql.append(filedName);
			sql.append("='");
			sql.append(record.getRowValueOfColumn(filedName));
			sql.append("' ");
			if (it.hasNext())
			{
				sql.append(" AND ");
			}
		}
		// 执行插入更新SQL语句
		try
		{
			Statement statement = connection.createStatement();
			statement.addBatch(sql.toString());
			statement.close();
		} catch (Exception e)
		{
			System.out.println(sql);
			throw new RuntimeException(e);
		}
	}

	/**
	 * 直接将Flink的Row数据类型转换为RowData使用Row中已有的数据类型
	 * @param row 待转化数据
	 * @return 转化后的RowData数据
	 */
	public static RowData convertFlinkRowToRowData(Row row)
	{
		GenericRowData rowData = new GenericRowData(row.getArity());
		for (int i = 0; i < row.getArity(); i++)
		{
			Object value = row.getField(i);
			if (value instanceof String)
			{
				rowData.setField(i, StringData.fromString((String) value));
			} else
			{
				rowData.setField(i, value);
			}
		}
		return rowData;
	}

	/**
	 * 为Connector创建一个合法的数据源Source
	 * @param name 名称
	 * @param source StreamSource可以是SourceFunction或者Source
	 * @return 一个合法的ScanTableSource
	 */
	public static <T> ScanTableSource createScanTableSource(String name, T source)
	{
		return new ScanTableSource()
		{
			@Override
			public String asSummaryString()
			{
				return name;
			}

			@Override
			public ChangelogMode getChangelogMode()
			{
				// 定义支持的记录操作类型
				ChangelogMode.Builder builder = ChangelogMode.newBuilder();
				builder.addContainedKind(RowKind.INSERT)
					   .addContainedKind(RowKind.UPDATE_BEFORE)
					   .addContainedKind(RowKind.UPDATE_AFTER)
					   .addContainedKind(RowKind.DELETE);
				return builder.build();
			}

			@Override
			public DynamicTableSource copy()
			{
				return this;
			}

			@Override
			public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext)
			{
				if (source instanceof Source)
				{
					return SourceProvider.of((Source<RowData, ?, ?>) source);
				} else if (source instanceof SourceFunction)
				{
					return SourceFunctionProvider.of((SourceFunction<RowData>) source, false);
				} else
				{
					throw new IllegalArgumentException("source必须传入一个有效的SourceFunction或者Source");
				}
			}
		};
	}

	/**
	 * 为Connector创建一个合法的数据源Sink
	 * @param name 名称
	 * @param sink StreamSink一般为SinkFunction
	 * @return 一个合法的DynamicTableSink
	 */
	public static DynamicTableSink createDynamicTableSink(String name, SinkFunction sink)
	{
		return new DynamicTableSink()
		{
			@Override
			public String asSummaryString()
			{
				return name;
			}

			@Override
			public DynamicTableSink copy()
			{
				return this;
			}

			@Override
			public ChangelogMode getChangelogMode(ChangelogMode changelogMode)
			{
				// 定义支持的记录操作类型
				ChangelogMode.Builder builder = ChangelogMode.newBuilder();
				builder.addContainedKind(RowKind.INSERT)
					   .addContainedKind(RowKind.UPDATE_BEFORE)
					   .addContainedKind(RowKind.UPDATE_AFTER)
					   .addContainedKind(RowKind.DELETE);
				return builder.build();
			}

			@Override
			public SinkRuntimeProvider getSinkRuntimeProvider(Context context)
			{
				return SinkFunctionProvider.of(sink);
			}
		};
	}
}
