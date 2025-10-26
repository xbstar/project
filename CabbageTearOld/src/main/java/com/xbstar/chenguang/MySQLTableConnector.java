package com.xbstar.chenguang;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.xbstar.FlinkTableSchema;
import com.xbstar.CabbageUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

/**
 * TableAPI中数据表的连接器
 */
public class MySQLTableConnector implements DynamicTableSourceFactory, DynamicTableSinkFactory
{
	// 创建用户必须配置的参数
	public static final ConfigOption<String> HOSTNAME = ConfigOptions.key("hostname").stringType().noDefaultValue();
	public static final ConfigOption<String> USERNAME = ConfigOptions.key("username").stringType().noDefaultValue();
	public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password").stringType().noDefaultValue();
	public static final ConfigOption<String> DATABASE_NAME = ConfigOptions.key("database").stringType().noDefaultValue();
	public static final ConfigOption<String> TABLE_NAME = ConfigOptions.key("table").stringType().noDefaultValue();
	// 创建用户选择配置的参数
	public static final ConfigOption<Integer> PORT = ConfigOptions.key("port").intType().defaultValue(3306);
	public static final ConfigOption<String> STARTUP_MODE = ConfigOptions.key("scan.startup.mode").stringType().defaultValue("initial");

	@Override
	public String factoryIdentifier()
	{
		return "mysql";
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions()
	{
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(HOSTNAME);
		options.add(USERNAME);
		options.add(PASSWORD);
		options.add(DATABASE_NAME);
		options.add(TABLE_NAME);
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions()
	{
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(PORT);
		options.add(STARTUP_MODE);
		return options;
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context)
	{
		// 校验并获取所有的配置项
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		helper.validate();
		ReadableConfig options = helper.getOptions();
		// 获取用户声明的表和字段定义
		ResolvedCatalogTable catalogTable = context.getCatalogTable();
		FlinkTableSchema schema = new FlinkTableSchema( options.get(TABLE_NAME), catalogTable.getResolvedSchema());
		// 使用FlinkCDC的StreamAPI接口构造MySQLSource
		StartupOptions startupOption = null;
		switch (options.get(STARTUP_MODE))
		{
			case "initial":
			{
				startupOption = StartupOptions.initial();
				break;
			}
			case "earliest":
			{
				startupOption = StartupOptions.earliest();
				break;
			}
			case "latest":
			{
				startupOption = StartupOptions.latest();
				break;
			}
			default:
			{
				throw new IllegalArgumentException("传入的scan.startup.mode值不合法！");
			}
		}
		MySqlSource<RowData> mySqlSource = MySqlSource.<RowData>builder()
				.hostname(options.get(HOSTNAME)) // 主机名
				.port(options.get(PORT)) //端口
				.username(options.get(USERNAME)) //用户名
				.password(options.get(PASSWORD)) //密码
				.databaseList(options.get(DATABASE_NAME)) // 设置捕获的数据库
				.tableList(options.get(DATABASE_NAME) + "." + options.get(TABLE_NAME)) // 设置捕获的表
				.deserializer(new MySQLSourceDebezium<>(options.get(HOSTNAME), options.get(PORT), options.get(DATABASE_NAME), options.get(USERNAME), options.get(PASSWORD), schema)) //原生的将 SourceRecord 转换为 JSON 字符串
				.startupOptions(startupOption) //启动配置初始化全部监听
				.serverTimeZone("Asia/Shanghai")
				.build();
		return CabbageUtils.createScanTableSource("FlinkCDCMySQLSource", mySqlSource);
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context)
	{
		// 校验并获取所有的配置项
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		helper.validate();
		ReadableConfig options = helper.getOptions();
		// 获取用户声明的表和字段定义
		ResolvedCatalogTable catalogTable = context.getCatalogTable();
		// context.getObjectIdentifier().getObjectName()
		FlinkTableSchema schema = new FlinkTableSchema( options.get(TABLE_NAME), catalogTable.getResolvedSchema());
		// 基于JDBC的SinkFunction构建Sink
		MySQLSinkFunction<RowData> sinkFunction = new MySQLSinkFunction<>(options.get(HOSTNAME), options.get(PORT), options.get(DATABASE_NAME), options.get(USERNAME), options.get(PASSWORD), schema);
		return CabbageUtils.createDynamicTableSink("MySQLSink", sinkFunction);
	}
}
