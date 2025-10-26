package com.xbstar.chenguang;

import com.xbstar.FlinkTableSchema;
import com.xbstar.CabbageUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class JDBCSinkFunction<T> extends RichSinkFunction<T>
{
	/** 配置JDBC链接需要的数据*/
	private FlinkTableSchema tableSchema; //FlinkRow数据结构从FlinkDynamicTable的Schema中获取
	private final String jdbcDriver;
	private final String hostName;
	private final String hostPort;
	private final String userName;
	private final String passWord;
	private final String dataBase;
	private final String tableName;
	private final Class<T> recordClass;
	/** 算子运行时生成的临时变量*/
	private boolean firstInvoke = true;
	private transient Connection currentJDBCConnection;

	JDBCSinkFunction(String jdbcDriver, String host, int port, String userName, String passWord, String dataBase, String tableName, FlinkTableSchema schema, Class<T> recordClass)
	{
		this.jdbcDriver = jdbcDriver;
		this.hostName = host;
		this.hostPort = String.valueOf(port);
		this.userName = userName;
		this.passWord = passWord;
		this.dataBase = dataBase;
		this.tableName = tableName;
		if (schema != null)
		{
			this.tableSchema = schema;
		}
		this.recordClass = recordClass;
	}

	public void open(Configuration parameters) throws Exception
	{
		/** 获得JDBC数据库连接*/
		Class.forName(this.jdbcDriver);// 注册 JDBC 驱动
		DriverManager.setLoginTimeout(5); //设置超时时间
		// 打开JDBC链接
		this.currentJDBCConnection = DriverManager.getConnection(this.getJDBCConnURL(), this.userName, this.passWord);
		// 关闭事务提交
		this.currentJDBCConnection.setAutoCommit(true);
		// 从数据库获得Schema
		if (this.tableSchema == null)
		{
			this.tableSchema = this.getJDBCSchema();
		}
		else
		{
			// 这种情况用户在程序中声明了Schema，需要与JDBC数据库进行比对
			this.compareSchema();
		}
	}

	private String getJDBCConnURL()
	{
		switch (this.jdbcDriver.trim())
		{
			case "com.mysql.cj.jdbc.Driver":
			{
				return "jdbc:mysql://" + hostName + ":" + hostPort + "/" + dataBase;
			}
			case "dm.jdbc.driver.DmDriver":
			{
				return "jdbc:dm://" + hostName + ":" + hostPort;
			}
			default:
			{
				System.out.println("\033[1;31m" + "指定的JDBC驱动不正确:" + this.jdbcDriver + "\033[0m");
				throw new IllegalArgumentException("指定的JDBC驱动不正确:" + this.jdbcDriver);
			}
		}
	}

	private FlinkTableSchema getJDBCSchema() throws SQLException
	{
		switch (this.jdbcDriver.trim())
		{
			case "com.mysql.cj.jdbc.Driver":
			{
				return CabbageUtils.getTableSchemaFormMySQL(this.currentJDBCConnection, tableName);
			}
			case "dm.jdbc.driver.DmDriver":
			{
				return CabbageUtils.getTableSchemaFromDM(this.currentJDBCConnection, tableName);
			}
			default:
			{
				System.out.println("\033[1;31m" + "指定的JDBC驱动不正确:" + this.jdbcDriver + "\033[0m");
				throw new IllegalArgumentException("指定的JDBC驱动不正确:" + this.jdbcDriver);
			}
		}
	}

	private void compareSchema() throws SQLException
	{
		FlinkTableSchema declareSchema = this.tableSchema;
		FlinkTableSchema jdbcSchema = this.getJDBCSchema();
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
		/* 在主键的约束方面，数据库中的主键都必须在用户声明中匹配到
		 * 但是反过来是允许的，用户声明的主键不一定要在数据库中
		 * 可能数据没有声明主键，但是DynamicTable中声明了，更新按照声明的主键来匹配记录
		 */
		Set<String> declarePKSet = declareSchema.getPrimaryKeyNames().stream().map(name -> name.toUpperCase()).collect(Collectors.toSet());
		for (String jdbcPK : jdbcSchema.getPrimaryKeyNames())
		{
			if (!declarePKSet.contains(jdbcPK.toUpperCase()))
			{
				System.out.println("\033[1;31m" + "数据库表 " + tableName + " 中的主键 " + jdbcPK + " 未在" + this.tableSchema
						.getTableName() + "中声明" + "\033[0m");
				throw new IllegalArgumentException("数据库表 " + tableName + " 中的主键 " + jdbcPK + " 未在" + this.tableSchema.getTableName() + "中声明");
			}
		}
		/* JDBC中的非空字段都必须在用户字段中声明*/
		Set<String> declareColumnNames = declareSchema.getRowColumnNames().stream().map(name -> name.toUpperCase()).collect(Collectors.toSet());
		for (String jdbcNotNull : jdbcSchema.getNotNullColumnNames())
		{
			if (!declareColumnNames.contains(jdbcNotNull.toUpperCase()))
			{
				System.out.println("\033[1;31m" + "数据库表 " + tableName + " 中的非空字段 " + jdbcNotNull + " 必须在" + this.tableSchema
						.getTableName() + "中声明" + "\033[0m");
				throw new IllegalArgumentException("数据库表 " + tableName + " 中的非空字段 " + jdbcNotNull + " 必须在" + this.tableSchema
						.getTableName() + "中声明");
			}
		}
	}

	@Override
	public void invoke(T value, Context context) throws Exception
	{
		// 统一转化为Row进行处理
		Row rowValue = null;
		if (value instanceof Row)
		{
			rowValue = (Row) value;
		}
		else if (value instanceof RowData)
		{
			rowValue = this.tableSchema.convertRowDataToRow((RowData) value);
		}
		else
		{
			System.out.println("\033[1;31m" + "流数据类型必须是Row,当前传入的是:" + rowValue.getClass().toString() + "\033[0m");
			throw new IllegalAccessException("流数据类型必须是Row,当前传入的是:" + rowValue.getClass().toString());
		}
		// 判断流数据中是否包含所有声明列的数据,由于大小写不敏感先构造rowValue大写的键到其键的映射
		Map<String, String> columnUpperNameToNameMap = new HashMap<>();
		for (String valName : rowValue.getFieldNames(true))
		{
			columnUpperNameToNameMap.put(valName.toUpperCase(), valName);
		}
		Row oldValue = rowValue;
		rowValue = Row.withPositions(rowValue.getKind(), this.tableSchema.getColumnSize()); //构造新的rowValue
		for (String columnName : this.tableSchema.getRowColumnNames())
		{
			if (columnUpperNameToNameMap.keySet().contains(columnName.toUpperCase()))
			{
				rowValue.setField(this.tableSchema.getPositionOfColumn(columnName), oldValue
						.getField(columnUpperNameToNameMap.get(columnName.toUpperCase())));
			}
			else
			{
				// 没有报错或者补null
				if (this.tableSchema.getNotNullColumnNames().contains(columnName))
				{
					System.out.println("\033[1;31m" + "传入的流数据源中不包含列 " + columnName + " 的数据" + "\033[0m");
					throw new IllegalArgumentException("传入的流数据源中不包含列 " + columnName + " 的数据");
				}
				else
				{
					rowValue.setField(this.tableSchema.getPositionOfColumn(columnName), null);
				}
			}
		}
		// 开始执行插入
		if (this.firstInvoke)
		{
			String dbName = "";
			if (this.jdbcDriver.equals("com.mysql.cj.jdbc.Driver"))
				dbName = "MySQL";
			if (this.jdbcDriver.equals("dm.jdbc.driver.DmDriver"))
				dbName = "达梦8";
			System.out.println("\033[1;36m" + "JDBCSinkFunction开始执行" + dbName + "数据库数据插入" + "\033[0m");
			this.firstInvoke = false;
		}
		switch (rowValue.getKind())
		{
			case INSERT:
			case UPDATE_AFTER:
			{
				// 构造插入更新语句
				switch (this.jdbcDriver.trim())
				{
					case "com.mysql.cj.jdbc.Driver":
					{
						this.execMySQLUpsert(rowValue);
						break;
					}
					case "dm.jdbc.driver.DmDriver":
					{
						this.execDM8Upsert(rowValue);
						break;
					}
				}
				break;
			}
			case DELETE:
			{
				StringBuilder sql = new StringBuilder();
				sql.append("delete from ");
				sql.append(this.tableName);
				sql.append(" where ");
				Iterator<String> it = this.tableSchema.getPrimaryKeyNames().iterator();
				while (it.hasNext())
				{
					String filedName = it.next();
					sql.append(filedName);
					sql.append("='");
					sql.append(rowValue.getField(this.tableSchema.getPositionOfColumn(filedName)));
					sql.append("' ");
					if (it.hasNext())
					{
						sql.append(" and ");
					}
				}
				this.currentJDBCConnection.createStatement().execute(sql.toString());
				break;
			}
		}
	}

	private void execDM8Upsert(Row rowValue) throws SQLException
	{
		// 构造插入SQL语句
		StringBuilder sql = new StringBuilder();
		sql.append("merge into " + this.dataBase + "." + this.tableName);
		sql.append(" using ( select ");
		Iterator<String> iterator = this.tableSchema.getRowColumnNames().iterator();
		while (iterator.hasNext())
		{
			sql.append("? " + iterator.next());
			if (iterator.hasNext())
			{
				sql.append(",");
			}
		}
		sql.append(" ) newline ");
		sql.append(" on (");
		iterator = this.tableSchema.getPrimaryKeyNames().iterator();
		while (iterator.hasNext())
		{
			String pkName = iterator.next();
			sql.append(this.tableName + "." + pkName + "=newline." + pkName);
			if (iterator.hasNext())
			{
				sql.append(" AND ");
			}
		}
		sql.append(" ) ");
		// 如果匹配则更新，注意更新不能更新主键字段
		Set<String> columnNames = this.tableSchema.getRowColumnNames();
		columnNames.removeAll(this.tableSchema.getPrimaryKeyNames());
		if (columnNames.size() > 0)
		{
			sql.append(" when matched then ");
			sql.append(" update set ");
			iterator = columnNames.iterator();
			while (iterator.hasNext())
			{
				String columnName = iterator.next();
				sql.append(this.tableName + "." + columnName + "=newline." + columnName);
				if (iterator.hasNext())
				{
					sql.append(" , ");
				}
			}
		}
		sql.append(" when not matched then ");
		sql.append(" insert (");
		iterator = this.tableSchema.getRowColumnNames().iterator();
		while (iterator.hasNext())
		{
			sql.append(iterator.next());
			if (iterator.hasNext())
			{
				sql.append(",");
			}
		}
		sql.append(" ) values (");
		iterator = this.tableSchema.getRowColumnNames().iterator();
		while (iterator.hasNext())
		{
			sql.append("newline." + iterator.next());
			if (iterator.hasNext())
			{
				sql.append(",");
			}
		}
		sql.append(")");
		// 执行插入更新SQL语句
		PreparedStatement ps = this.currentJDBCConnection.prepareStatement(sql.toString());
		for (int i = 0; i < this.tableSchema.getColumnSize(); i++)
		{
			ps.setObject(i + 1, rowValue.getField(i));
		}
		ps.execute();
		ps.close();
	}

	private void execMySQLUpsert(Row rowValue) throws Exception
	{
		// 构造插入SQL语句
		StringBuilder sql = new StringBuilder();
		sql.append("replace into ");
		sql.append(this.tableName + "(");
		Iterator<String> it = this.tableSchema.getRowColumnNames().iterator();
		while (it.hasNext())
		{
			sql.append(it.next());
			if (it.hasNext())
			{
				sql.append(",");
			}
		}
		sql.append(") values (");
		it = this.tableSchema.getRowColumnNames().iterator();
		while (it.hasNext())
		{
			sql.append("?");
			it.next();
			if (it.hasNext())
			{
				sql.append(",");
			}
		}
		sql.append(")");
		// 执行插入更新SQL语句
		PreparedStatement ps = this.currentJDBCConnection.prepareStatement(sql.toString());
		for (int i = 0; i < this.tableSchema.getColumnSize(); i++)
		{
			ps.setObject(i + 1, rowValue.getField(i));
		}
		ps.execute();
		ps.close();
	}

	public void close() throws Exception
	{
		// 即使invoke抛出异常，close也一定会调用
		if (this.currentJDBCConnection.getAutoCommit() == false)
		{
			this.currentJDBCConnection.commit(); //把所有的事务都提交
		}
		this.currentJDBCConnection.setAutoCommit(true);// 如果此连接不释放的话，则将自动提交模式改回true
		this.currentJDBCConnection.close();
	}
}
