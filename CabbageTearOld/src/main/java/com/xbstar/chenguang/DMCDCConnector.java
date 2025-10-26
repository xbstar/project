package com.xbstar.chenguang;

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

public class DMCDCConnector implements DynamicTableSourceFactory, DynamicTableSinkFactory
{
	// 创建用户必须配置的参数
	public static final ConfigOption<String> HOSTNAME = ConfigOptions.key("hostname").stringType().noDefaultValue();
	public static final ConfigOption<String> USERNAME = ConfigOptions.key("username").stringType().noDefaultValue();
	public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password").stringType().noDefaultValue();
	public static final ConfigOption<String> DATABASE_NAME = ConfigOptions.key("database").stringType().noDefaultValue();
	public static final ConfigOption<String> TABLE_NAME = ConfigOptions.key("table").stringType().noDefaultValue();
	// 创建用户选择配置的参数
	public static final ConfigOption<Integer> PORT = ConfigOptions.key("port").intType().defaultValue(5236);

	@Override
	public String factoryIdentifier()
	{
		return "dm8";
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
		return options;
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context)
	{
		// 校验并获取所有的配置项
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		helper.validate();
		ReadableConfig options = helper.getOptions();
		// 创建FlinkCDCSource
		ResolvedCatalogTable catalogTable = context.getCatalogTable();
		FlinkTableSchema schema = new FlinkTableSchema( options.get(TABLE_NAME),catalogTable.getResolvedSchema());
		DMSourceFunction<RowData> dmSource = new DMSourceFunction(options.get(PORT), options.get(HOSTNAME), options.get(USERNAME), options
				.get(PASSWORD), options.get(DATABASE_NAME), options.get(TABLE_NAME), schema, RowData.class);
		return CabbageUtils.createScanTableSource("DM8Source", dmSource);
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
		FlinkTableSchema schema = new FlinkTableSchema( options.get(TABLE_NAME),catalogTable.getResolvedSchema());
		// 基于JDBC的SinkFunction构建Sink
		JDBCSinkFunction<RowData> sinkFunction = new JDBCSinkFunction<>("dm.jdbc.driver.DmDriver",
				options.get(HOSTNAME),
				options.get(PORT),
				options.get(USERNAME),
				options.get(PASSWORD),
				options.get(DATABASE_NAME),
				options.get(TABLE_NAME), schema, RowData.class);
		return CabbageUtils.createDynamicTableSink("DM8Sink", sinkFunction);
	}
}
