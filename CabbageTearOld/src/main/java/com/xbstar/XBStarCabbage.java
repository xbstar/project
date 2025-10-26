package com.xbstar;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.module.ModuleEntry;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.types.Row;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class XBStarCabbage
{
	/** 西北星流处理变量，已经进行优化并行度默认为1，可以直接import*/
	public static XBStarStreamExecutionEnvironment env;
	public static XBStarTableExecutionEnvironment tnv;

	static
	{
		// Flink运行时的配置
		Map<String, String> flinkRunTimeConfig = new HashMap<>();
		flinkRunTimeConfig.put("rest.bind-port","8081");
		// 使用Java的静态代码块语法进行初始化，在此类加载的时候就会执行
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(Configuration.fromMap(flinkRunTimeConfig));
		env.setRestartStrategy(RestartStrategies.noRestart()); //出现错误不重启报错退出
		env.setParallelism(1); //并行度默认为1
		env.disableOperatorChaining(); //设置算子不自动合并优化
		CheckpointConfig.ExternalizedCheckpointCleanup retainOnCancel = CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;
		env.getCheckpointConfig().setExternalizedCheckpointCleanup(retainOnCancel); //手动取消任务保留检查点
		// 构造公共引用变量
		XBStarCabbage.env = new XBStarStreamExecutionEnvironment(env);
		XBStarCabbage.tnv = new XBStarTableExecutionEnvironment(env);
	}

	/** 包装原生StreamExecutionEnvironment形成env的基础类*/
	public static class XBStarStreamExecutionEnvironment
	{
		private StreamExecutionEnvironment flinkStreamEnv;

		public XBStarStreamExecutionEnvironment(StreamExecutionEnvironment flinkStremEnv)
		{
			this.flinkStreamEnv = flinkStremEnv;
		}

		/** 获取Flink原生的StreamExecutionEnvironment执行环境*/
		public StreamExecutionEnvironment getOriginal()
		{
			return this.flinkStreamEnv;
		}

		public StreamExecutionEnvironment setParallelism(int parallelism)
		{
			return this.flinkStreamEnv.setParallelism(parallelism);
		}

		public StreamExecutionEnvironment enableCheckpointing(long interval)
		{
			return this.flinkStreamEnv.enableCheckpointing(interval);
		}

		public JobExecutionResult execute() throws Exception
		{
			return this.flinkStreamEnv.execute();
		}

		public JobExecutionResult execute(String jobName) throws Exception
		{
			return this.flinkStreamEnv.execute(jobName);
		}

		public DataStreamSource<Long> fromSequence(long from, long to)
		{
			return this.flinkStreamEnv.fromSequence(from, to);
		}

		public final <OUT> DataStreamSource<OUT> fromElements(OUT... data)
		{
			return this.flinkStreamEnv.fromElements(data);
		}

		public <OUT> DataStreamSource<OUT> fromCollection(Collection<OUT> data)
		{
			return this.flinkStreamEnv.fromCollection(data);
		}

		public <OUT> DataStreamSource<OUT> fromCollection(Collection<OUT> data,TypeInformation<OUT> type)
		{
			return this.flinkStreamEnv.fromCollection(data,type);
		}

		public <OUT> DataStreamSource<OUT> addSource(SourceFunction<OUT> function)
		{
			return this.flinkStreamEnv.addSource(function);
		}

		public <OUT> DataStreamSource<OUT> addSource(SourceFunction<OUT> function, String sourceName)
		{
			return this.flinkStreamEnv.addSource(function, sourceName);
		}

		public <OUT> DataStreamSource<OUT> addSource(SourceFunction<OUT> function, TypeInformation<OUT> typeInfo)
		{
			return this.flinkStreamEnv.addSource(function, typeInfo);
		}

		public <OUT> DataStreamSource<OUT> fromSource(Source<OUT, ?, ?> source)
		{
			return this.fromSource(source, WatermarkStrategy.noWatermarks(), "FlinkSource");
		}

		public <OUT> DataStreamSource<OUT> fromSource(Source<OUT, ?, ?> source, WatermarkStrategy<OUT> timestampsAndWatermarks, String sourceName)
		{
			return this.flinkStreamEnv.fromSource(source, timestampsAndWatermarks, sourceName);
		}

		public <OUT> DataStreamSource<OUT> fromSource(Source<OUT, ?, ?> source, WatermarkStrategy<OUT> timestampsAndWatermarks, String sourceName, TypeInformation<OUT> typeInfo)
		{
			return this.flinkStreamEnv.fromSource(source, timestampsAndWatermarks, sourceName, typeInfo);
		}
	}

	/**
	 * 对原生的FlinkTable运行时环境进行再封装和改造
	 * 1.增加init接口，用于执行SQL语句
	 */
	public static class XBStarTableExecutionEnvironment implements TableEnvironment
	{
		private StreamExecutionEnvironment flinkStreamEnv;
		private StreamTableEnvironment flinkTableEnv;

		public XBStarTableExecutionEnvironment(StreamExecutionEnvironment flinkStreamEnv)
		{
			this.flinkStreamEnv = flinkStreamEnv;
			this.flinkTableEnv = StreamTableEnvironment.create(this.flinkStreamEnv);
		}

		/**
		 * 执行初始化SQL语句，默认不打印
		 * @param path SQL文件编译路径
		 * @throws Exception 抛出语句执行异常
		 */
		public void init(String path) throws Exception
		{
			init(path, false);
		}

		/**
		 * 执行初始化SQL语句，配置是否打印
		 * @param path SQL文件编译路径
		 * @param print 是否在执行是打印语句
		 * @throws Exception 抛出语句执行异常
		 */
		public void init(String path, boolean print) throws Exception
		{
			URL resource = ClassLoader.getSystemResource(path);
			// 以行为单位读取文件内容，一次读一整行;
			BufferedReader reader = new BufferedReader(new InputStreamReader((InputStream) resource.getContent()));
			// 文件内容
			StringBuilder sb = new StringBuilder();
			String tempString = null;
			// 一次读入一行，直到读入null为文件结束
			while ((tempString = reader.readLine()) != null)
			{
				if (tempString.trim().startsWith("#") || tempString.trim().startsWith("--"))
				{
					continue;
				}
				sb.append(tempString).append(" ");
			}
			reader.close();
			// 通过分号分割带执行的SQL语句
			String[] result = sb.toString().trim().split(";");
			// 依次处理每一行SQL
			for (String sql : result)
			{
				if (print)
				{
					System.out.println(sql.trim());
				}
				// 执行SQL语句
				tnv.executeSql(sql);
			}
		}

		public StreamExecutionEnvironment setParallelism(int parallelism)
		{
			return this.flinkStreamEnv.setParallelism(parallelism);
		}

		public JobExecutionResult execute() throws Exception
		{
			return this.flinkStreamEnv.execute();
		}

		public JobExecutionResult execute(String jobName) throws Exception
		{
			return this.flinkStreamEnv.execute(jobName);
		}

		public DataStream<Row> toDataStream(Table table)
		{
			return this.flinkTableEnv.toChangelogStream(table);
		}

		public StreamExecutionEnvironment enableCheckpointing(long interval)
		{
			return this.flinkStreamEnv.enableCheckpointing(interval);
		}

		@Override
		public Table fromValues(Expression... expressions)
		{
			return this.flinkTableEnv.fromValues(expressions);
		}

		@Override
		public Table fromValues(AbstractDataType<?> abstractDataType, Expression... expressions)
		{
			return this.flinkTableEnv.fromValues(abstractDataType, expressions);
		}

		@Override
		public Table fromValues(Iterable<?> iterable)
		{
			return this.flinkTableEnv.fromValues(iterable);
		}

		@Override
		public Table fromValues(AbstractDataType<?> abstractDataType, Iterable<?> iterable)
		{
			return this.flinkTableEnv.fromValues(abstractDataType, iterable);
		}

		@Override
		public void registerCatalog(String s, Catalog catalog)
		{
			this.flinkTableEnv.registerCatalog(s, catalog);
		}

		@Override
		public Optional<Catalog> getCatalog(String s)
		{
			return this.flinkTableEnv.getCatalog(s);
		}

		@Override
		public void loadModule(String s, Module module)
		{
			this.flinkTableEnv.loadModule(s, module);
		}

		@Override
		public void useModules(String... strings)
		{
			this.flinkTableEnv.useModules(strings);
		}

		@Override
		public void unloadModule(String s)
		{
			this.flinkTableEnv.unloadModule(s);
		}

		@Override
		public void registerFunction(String s, ScalarFunction scalarFunction)
		{
			this.flinkTableEnv.registerFunction(s, scalarFunction);
		}

		@Override
		public void createTemporarySystemFunction(String s, Class<? extends UserDefinedFunction> aClass)
		{
			this.flinkTableEnv.createTemporarySystemFunction(s, aClass);
		}

		@Override
		public void createTemporarySystemFunction(String s, UserDefinedFunction userDefinedFunction)
		{
			this.flinkTableEnv.createTemporarySystemFunction(s, userDefinedFunction);
		}

		@Override
		public boolean dropTemporarySystemFunction(String s)
		{
			return this.flinkTableEnv.dropTemporarySystemFunction(s);
		}

		@Override
		public void createFunction(String s, Class<? extends UserDefinedFunction> aClass)
		{
			this.flinkTableEnv.createFunction(s, aClass);
		}

		@Override
		public void createFunction(String s, Class<? extends UserDefinedFunction> aClass, boolean b)
		{
			this.flinkTableEnv.createFunction(s, aClass, b);
		}

		@Override
		public boolean dropFunction(String s)
		{
			return this.flinkTableEnv.dropFunction(s);
		}

		@Override
		public void createTemporaryFunction(String s, Class<? extends UserDefinedFunction> aClass)
		{
			this.flinkTableEnv.createTemporaryFunction(s, aClass);
		}

		@Override
		public void createTemporaryFunction(String s, UserDefinedFunction userDefinedFunction)
		{
			this.flinkTableEnv.createTemporaryFunction(s, userDefinedFunction);
		}

		@Override
		public boolean dropTemporaryFunction(String s)
		{
			return this.flinkTableEnv.dropTemporaryFunction(s);
		}

		@Override
		public void createTemporaryTable(String s, TableDescriptor tableDescriptor)
		{
			this.flinkTableEnv.createTemporaryTable(s, tableDescriptor);
		}

		@Override
		public void createTable(String s, TableDescriptor tableDescriptor)
		{
			this.flinkTableEnv.createTable(s, tableDescriptor);
		}

		@Override
		public void registerTable(String s, Table table)
		{
			this.flinkTableEnv.registerTable(s, table);
		}

		@Override
		public void createTemporaryView(String s, Table table)
		{
			this.flinkTableEnv.createTemporaryView(s, table);
		}

		@Override
		public Table scan(String... strings)
		{
			return this.flinkTableEnv.scan(strings);
		}

		@Override
		public Table from(String s)
		{
			return this.flinkTableEnv.from(s);
		}

		@Override
		public Table from(TableDescriptor tableDescriptor)
		{
			return this.flinkTableEnv.from(tableDescriptor);
		}

		@Override
		public String[] listCatalogs()
		{
			return this.flinkTableEnv.listCatalogs();
		}

		@Override
		public String[] listModules()
		{
			return this.flinkTableEnv.listModules();
		}

		@Override
		public ModuleEntry[] listFullModules()
		{
			return this.flinkTableEnv.listFullModules();
		}

		@Override
		public String[] listDatabases()
		{
			return this.flinkTableEnv.listDatabases();
		}

		@Override
		public String[] listTables()
		{
			return this.flinkTableEnv.listTables();
		}

		@Override
		public String[] listTables(String s, String s1)
		{
			return this.flinkTableEnv.listTables();
		}

		@Override
		public String[] listViews()
		{
			return this.flinkTableEnv.listViews();
		}

		@Override
		public String[] listTemporaryTables()
		{
			return this.flinkTableEnv.listTemporaryTables();
		}

		@Override
		public String[] listTemporaryViews()
		{
			return this.flinkTableEnv.listTemporaryViews();
		}

		@Override
		public String[] listUserDefinedFunctions()
		{
			return this.flinkTableEnv.listUserDefinedFunctions();
		}

		@Override
		public String[] listFunctions()
		{
			return this.flinkTableEnv.listFunctions();
		}

		@Override
		public boolean dropTemporaryTable(String s)
		{
			return this.flinkTableEnv.dropTemporaryTable(s);
		}

		@Override
		public boolean dropTemporaryView(String s)
		{
			return this.flinkTableEnv.dropTemporaryView(s);
		}

		@Override
		public String explainSql(String s, ExplainDetail... explainDetails)
		{
			return this.flinkTableEnv.explainSql(s, explainDetails);
		}

		@Override
		public String[] getCompletionHints(String s, int i)
		{
			return this.flinkTableEnv.getCompletionHints(s, i);
		}

		@Override
		public Table sqlQuery(String s)
		{
			return this.flinkTableEnv.sqlQuery(s);
		}

		@Override
		public TableResult executeSql(String s)
		{
			return this.flinkTableEnv.executeSql(s);
		}

		@Override
		public String getCurrentCatalog()
		{
			return this.flinkTableEnv.getCurrentCatalog();
		}

		@Override
		public void useCatalog(String s)
		{
			this.flinkTableEnv.useCatalog(s);
		}

		@Override
		public String getCurrentDatabase()
		{
			return this.flinkTableEnv.getCurrentDatabase();
		}

		@Override
		public void useDatabase(String s)
		{
			this.flinkTableEnv.useDatabase(s);
		}

		@Override
		public TableConfig getConfig()
		{
			return this.flinkTableEnv.getConfig();
		}

		@Override
		public StatementSet createStatementSet()
		{
			return this.flinkTableEnv.createStatementSet();
		}

		@Override
		public CompiledPlan loadPlan(PlanReference planReference) throws TableException
		{
			return this.flinkTableEnv.loadPlan(planReference);
		}

		@Override
		public CompiledPlan compilePlanSql(String s) throws TableException
		{
			return this.flinkTableEnv.compilePlanSql(s);
		}
	}
}
