package com.xbstar.chenguang;

import com.ibm.icu.impl.IllegalIcuArgumentException;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.xbstar.FlinkTableRow;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.util.HashSet;
import java.util.Set;

/**
 * FlinkStreamAPI的MySQL数据库的算子构造辅助类
 * 能够生成MySQL双向（Source和Sink）的Stream标准算子
 * 支持MySQL多表捕获和全库捕获
 */
public class MySQLStreamOperator
{
	private int port = 3306;
	private String host;
	private String database;
	private String userName;
	private String passWord;
	private Set<String> table;
	private Connection connection;

	/** 配置MySQL数据库的端口，一般不改不用配 ，默认是3306*/
	public MySQLStreamOperator port(int port)
	{
		this.port = port;
		return this;
	}

	/** 配置MySQL数据库的地址，必须配置，否则报错*/
	public MySQLStreamOperator host(String host)
	{
		if (host == null)
		{
			throw new IllegalArgumentException("host不能配置为null或者空串");
		}
		this.host = host;
		return this;
	}

	/** 配置MySQL数据库的数据库，必须配置，否则报错*/
	public MySQLStreamOperator database(String database)
	{
		if (database == null)
		{
			throw new IllegalArgumentException("database不能配置为null或者空串");
		}
		this.database = database;
		return this;
	}

	/** 配置达梦数据库的用户名，必须配置，否则报错*/
	public MySQLStreamOperator username(String userName)
	{
		if (userName == null)
		{
			throw new IllegalArgumentException("userName不能配置为null或者空串");
		}
		this.userName = userName;
		return this;
	}

	/** 配置达梦数据库的密码，必须配置，否则报错*/
	public MySQLStreamOperator password(String passWord)
	{
		if (passWord == null)
		{
			throw new IllegalArgumentException("passWord不能配置为null或者空串");
		}
		this.passWord = passWord;
		return this;
	}

	/**
	 * 配置监控MySQL数据库的Table表：
	 * 1.如果不配置则捕获所有的表
	 * 2.可以多次调用，则捕获多张表
	 * 3.如果只调用一次，则捕获指定的表
	 * */
	public MySQLStreamOperator table(String table)
	{
		if (table == null || "".equals(table.trim()))
		{
			throw new IllegalArgumentException("table不能配置为null或者空串");
		}
		if (this.table == null)
		{
			this.table = new HashSet<>();
		}
		this.table.add(table);
		return this;
	}

	/** 检查用户配置是否合法*/
	public void checkConfiguration(Class recordClass)
	{
		if (this.host == null || "".equals(this.host.trim()))
		{
			throw new IllegalArgumentException("必须配置host");
		}
		if (this.database == null || "".equals(this.database.trim()))
		{
			throw new IllegalArgumentException("必须配置database");
		}
		if (this.userName == null || "".equals(this.userName.trim()))
		{
			throw new IllegalArgumentException("必须配置username");
		}
		if (this.passWord == null || "".equals(this.passWord.trim()))
		{
			throw new IllegalArgumentException("必须配置password");
		}
		// 根据不同的情况检查用户表配置的合理性
		if (recordClass == FlinkTableRow.class)
		{
			// 用户开启了多表支持的情况
			if (this.table != null)
			{
				if (this.table.size() == 0)
				{
					// 这不可能不掉table接口为null，掉了则size至少是1
					throw new IllegalIcuArgumentException("请至少配置1张数据传输表");
				}
				if (this.table.contains(null))
				{
					// 这也不可能，在table接口调用的时候不能已经做了判断不能传入null
					throw new IllegalIcuArgumentException("数据表配置不能包含为空的表");
				}
			}
		} else
		{
			if (this.table == null || this.table.size() > 1)
			{
				throw new IllegalIcuArgumentException("Flink标准Row类型必须且仅支持配置捕获1张数据表（调用1次table接口），如果需要捕获多张请尝试buildMultiSource");
			}
		}
	}

	/** 返回MySQL单表StreamAPI的Source算子*/
	public MySqlSource<Row> buildSource()
	{
		this.checkConfiguration(Row.class);
		String tableName = this.table.iterator().next();
		MySqlSourceBuilder<Row> mysqlDebeziumSourceBuilder = MySqlSource.<Row>builder()
				.hostname(host) //主机名
				.port(port) //端口
				.username(userName) //用户名
				.password(passWord) //密码
				.databaseList(database) // 设置捕获的数据库
				.tableList(database + "." + tableName) //设置捕获的表
				.deserializer(new MySQLSourceDebezium<>(host, port, database, userName, passWord, tableName))// 设置捕获转化器
				.startupOptions(StartupOptions.initial()); //启动配置初始化全部监听
		return mysqlDebeziumSourceBuilder.build();
	}

	/** 返回MySQL多表StreamAPI的Source算子*/
	public MySqlSource<FlinkTableRow> buildMultiTableSource()
	{
		this.checkConfiguration(FlinkTableRow.class);
		MySqlSourceBuilder<FlinkTableRow> mysqlDebeziumSourceBuilder = MySqlSource.<FlinkTableRow>builder()
				.hostname(host) // 主机名
				.port(port) //端口
				.username(userName) //用户名
				.password(passWord) //密码
				.databaseList(database) // 设置捕获的数据库
				.deserializer(new MySQLSourceDebezium<>(host, port, database, userName, passWord))// 设置捕获转化器
				.startupOptions(StartupOptions.initial()); //启动配置初始化全部监听
		// 设置捕获的表
		if (this.table != null)
		{
			String[] tables = this.table.stream().map(table -> database + "." + table).toArray(size -> new String[size]);
			mysqlDebeziumSourceBuilder.tableList(tables);
		} else
		{
			mysqlDebeziumSourceBuilder.tableList(database + ".*");
		}
		return mysqlDebeziumSourceBuilder.build();
	}

	/** 返回MySQL单表StreamAPI的Sink算子*/
	public SinkFunction<Row> buildSink()
	{
		this.checkConfiguration(Row.class);
		// Row.class模式下只允许用户配置一张表，上面的checkConfiguration保证了这一点
		String tableName = this.table.iterator().next();
		return new MySQLSinkFunction<>(host, port, database, userName, passWord, tableName);
	}

	/** 返回MySQL多表StreamAPI的Sink算子*/
	public SinkFunction<FlinkTableRow> buildMultiTableSink()
	{
		this.checkConfiguration(FlinkTableRow.class);
		return new MySQLSinkFunction(host, port, database, userName, passWord);
	}



	/** 返回黑洞算子，不做任何的处理*/
	public static <T>SinkFunction<T> buildBlackHoleSink()
	{
		return new BlackHoleSink();
	}
}
