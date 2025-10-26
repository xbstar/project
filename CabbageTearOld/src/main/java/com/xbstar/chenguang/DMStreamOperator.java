package com.xbstar.chenguang;

import com.xbstar.FlinkTableSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.types.Row;

/* DM的增量实时捕获SourceFunction的类定义*/
public class DMStreamOperator
{
	private int port = 5236;
	private String host;
	private String database;
	private String table;
	private String userName;
	private String passWord;
	private FlinkTableSchema schema;

	/** 配置监控达梦数据库的地址，必须配置，否则报错*/
	public DMStreamOperator host(String host)
	{
		this.host = host;
		return this;
	}

	/** 配置达梦数据库的用户名，必须配置，否则报错*/
	public DMStreamOperator username(String userName)
	{
		this.userName = userName;
		return this;
	}

	/** 配置达梦数据库的密码，必须配置，否则报错*/
	public DMStreamOperator password(String passWord)
	{
		this.passWord = passWord;
		return this;
	}

	/** 配置监控达梦数据库的端口，非必须配置，不配就是5236*/
	public DMStreamOperator port(int port)
	{
		this.port = port;
		return this;
	}

	/** 配置监控达梦数据库的模式（相当于数据库），必粗配置否则报错*/
	public DMStreamOperator database(String database)
	{
		this.database = database;
		return this;
	}

	/** 配置监控达梦数据库的Table表，必须配置否则报错，并且目前只支持单表*/
	public DMStreamOperator table(String table)
	{
		this.table = table;
		return this;
	}

	/** 配置监控表格的字段，非必须，若不配置则从JDBC中读取表的字段*/
	public DMStreamOperator schema(ResolvedSchema schema)
	{
		this.schema = new FlinkTableSchema(table,schema);
		return this;
	}

	public DMStreamOperator schema(FlinkTableSchema schema)
	{
		this.schema = schema;
		return this;
	}

	private void check()
	{
		if (this.host == null || "".equals(this.host.trim()))
		{
			throw new IllegalArgumentException("必须配置host");
		}
		if (this.database == null || "".equals(this.database.trim()))
		{
			throw new IllegalArgumentException("必须配置database");
		}
		if (this.table == null || "".equals(this.table.trim()))
		{
			throw new IllegalArgumentException("必须配置table");
		}
		if (this.userName == null || "".equals(this.userName.trim()))
		{
			throw new IllegalArgumentException("必须配置username");
		}
		if (this.passWord == null || "".equals(this.passWord.trim()))
		{
			throw new IllegalArgumentException("必须配置password");
		}
	}

	public DMSourceFunction<Row> buildSource()
	{
		this.check();
		return new DMSourceFunctionRow(port, host, userName, passWord, database, table, schema, Row.class);
	}

	public JDBCSinkFunction<Row> buildSink()
	{
		this.check();
		return new JDBCSinkFunction<Row>("dm.jdbc.driver.DmDriver", host, port, userName, passWord, database, table, schema, Row.class);
	}

	/** 不能直接new DMSourceFunction<Row>否则抛出异常,Flink应该有技术来在运行时读取类的泛型，并作出了限制*/
	class DMSourceFunctionRow extends DMSourceFunction<Row>
	{
		DMSourceFunctionRow(int port, String hostName, String userName, String password, String databaseName, String tableName, FlinkTableSchema tableSchema, Class<Row> recordClass)
		{
			super(port, hostName, userName, password, databaseName, tableName, tableSchema, recordClass);
		}
	}
}
