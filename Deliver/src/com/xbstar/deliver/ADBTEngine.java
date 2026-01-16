package com.xbstar.deliver;

import com.xbstar.deliver.anno.*;
import com.xbstar.types.Column;
import com.xbstar.utils.ConvertUtils;
import com.xbstar.utils.EnvUtils;
import com.xbstar.utils.JDBCUtils;
import com.xbstar.utils.PrintUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class ADBTEngine
{
	public static String Package = EnvUtils.getConfigFromEnvironment(String.class, "deliver.package");
	public static String SourceAddress = EnvUtils.getConfigFromEnvironment(String.class, "deliver.source");
	public static String JDBCAddress = EnvUtils.getConfigFromEnvironment(String.class, "deliver.jdbc");
	public static String Username = EnvUtils.getConfigFromEnvironment(String.class, "deliver.username");
	public static String Password = EnvUtils.getConfigFromEnvironment(String.class, "deliver.password");
	public static Connection JDBCConnection;// 持有一个连接到目标数据库的静态连接
	public static Map<String, List<Column>> TableColumnsMap = new HashMap<>();// 缓存所有的表及其字段

	public static void generateADBT() throws IOException
	{
		if (Package == null) throw new RuntimeException("[Deliver]没有配置deliver.package");
		PrintUtils.printInfo("[Deliver]读取到配置deliver.package=" + Package);
		if (SourceAddress == null) throw new RuntimeException("[Deliver]没有配置deliver.source");
		PrintUtils.printInfo("[Deliver]读取到配置deliver.source=" + SourceAddress);
		// 计算ADBT的存储文件夹
		String path = SourceAddress;
		for (String pa : Package.split("\\.")) path += "/" + pa;
		File dir = new File(path);
		PrintUtils.printInfo("[Deliver]即将生成ADBT文件在 " + dir.getAbsolutePath());
		// 便利所有的数据库表
		Set<String> tableSet = JDBCUtils.getTableSet(getJDBCConnection());
		for (String table : tableSet)
		{
			String fileName = ConvertUtils.toCamelCase(table);
			File file = new File(dir, fileName + ".java");
			String fileInfo = "\u001b[1;94m" + "[Deliver]开始写入文件 " + fileName + ".java" + "\u001b[0m";
			if (file.exists())
			{
				String backTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
				fileInfo += "\u001b[1;33m" + " 将同名文件备份为" + fileName + "." + backTime + "\u001b[0m";
				file.renameTo(new File(dir, fileName + "." + backTime));
			}
			System.out.println(fileInfo);
			PrintWriter writer = new PrintWriter(new FileWriter(file));
			writer.println("package " + Package + ";");
			writer.println();
			writer.println("import com.xbstar.deliver.anno.Table;");
			writer.println("import com.xbstar.deliver.anno.PrimaryKey;");
			writer.println();
			writer.println("@Table(\"" + table + "\")");
			writer.println("public enum " + fileName);
			writer.println("{");
			Iterator<Column> columnIterator = JDBCUtils.getTableColumns(getJDBCConnection(), table).iterator();
			while (columnIterator.hasNext())
			{
				Column column = columnIterator.next();
				writer.print("\t");
				if (column.primary) writer.print("@PrimaryKey ");
				writer.print(column.field);
				if (columnIterator.hasNext()) writer.print(",");
				writer.println();
			}
			writer.println("}");
			writer.close();
		}
	}

	public static Connection getJDBCConnection()
	{
		if (JDBCAddress == null) throw new RuntimeException("[Deliver]没有配置deliver.jdbc");
		if (JDBCConnection == null) try {JDBCConnection = DriverManager.getConnection(JDBCAddress, Username, Password);} catch (Exception e) {throw new RuntimeException(e);}
		return JDBCConnection;
	}

	private static <T extends Enum> String inflateTableMetedata(Class<T> enumClass)
	{
		String tableName = getRealTableName(enumClass);
		if (TableColumnsMap.get(tableName) != null) return tableName; //已经判断过了直接返回
		List<Column> tableColumns = JDBCUtils.getTableColumns(getJDBCConnection(), tableName);//用JDBCUtils获取到实际表的元数据
		TableColumnsMap.put(tableName, tableColumns); //保存到Map结构中
		// 校验主键定义是否匹配真实字段
		List<String> currentPrimaryKeys = tableColumns.stream().filter(cur -> cur.primary).map(cur -> cur.field).collect(Collectors.toList());
		Set<String> declarePrimaryKeys = getADBTPrimarySet(enumClass).stream().map(cur -> cur.name()).collect(Collectors.toSet());
		for (String current : declarePrimaryKeys) if (!currentPrimaryKeys.contains(current)) throw new RuntimeException("[Deliver]" + enumClass.getSimpleName() + "定义的" + current + "字段在" + tableName + "中不是主键因此不应被标记为主键");
		for (String current : currentPrimaryKeys) if (!declarePrimaryKeys.contains(current)) throw new RuntimeException("[Deliver]" + enumClass.getSimpleName() + "定义的" + current + "字段在" + tableName + "表中是主键因此应当被标记为主键");
		//		if(currentPrimaryKeys.size()<=0)throw new RuntimeException("[Deliver]目前不支持处理无主键表" + tableName);
		// 校验定义字段定义是否匹配表真实字段
		List<String> currentFields = tableColumns.stream().map(cur -> cur.field).collect(Collectors.toList());
		List<String> declareFields = getADBTFieldMap(enumClass).values().stream().map(cur -> cur.substring(cur.lastIndexOf(".") + 1)).collect(Collectors.toList());
		for (String current : declareFields) if (!currentFields.contains(current)) throw new RuntimeException("[Deliver]" + enumClass.getSimpleName() + "关联的" + tableName + "表中不存在字段" + current);
		for (String current : currentFields) if (!declareFields.contains(current) && tableColumns.get(currentFields.indexOf(current)).notnull) throw new RuntimeException("[Deliver]" + enumClass.getSimpleName() + "中必须声明" + tableName + "中的非空字段" + current);
		return tableName;
	}

	public static <T extends Enum<?>> Boolean isRealTable(Class<T> enumClass)
	{
		if (enumClass == null) throw new RuntimeException("[Deliver]必须传入有效的ADBT枚举类型");
		Table table = enumClass.getAnnotation(Table.class);
		if (table != null) return true;
		View view = enumClass.getAnnotation(View.class);
		if (view != null) return false;
		throw new RuntimeException("[Deliver]ADBT必须配置有效的注解@Table或者@View)");
	}

	public static <T extends Enum<?>> String getRealTableName(Class<T> enumClass)
	{
		if (!isRealTable(enumClass)) throw new RuntimeException("[Deliver]View视图无法获取到实际表名");
		Table table = enumClass.getAnnotation(Table.class);
		String tableName = table.value();
		if (TableColumnsMap.keySet().contains(tableName)) return tableName;
		if (JDBCUtils.getTableExist(getJDBCConnection(), tableName)) return tableName;
		else throw new RuntimeException("[Deliver]" + JDBCUtils.getJDBCAddress(getJDBCConnection()) + "中没有名为" + tableName + "的表");
	}

	public static <T extends Enum> String getViewBaseTable(Class<T> enumClass)
	{
		if (isRealTable(enumClass)) throw new RuntimeException("[Deliver]Table表无法获取到视图基础表");
		View view = enumClass.getAnnotation(View.class);
		Class<? extends Enum> baseADBT = view.value();
		return getRealTableName(baseADBT);
	}

	public static <T extends Enum> Map<String, List<String>> getViewLeftJoin(Class<T> enumClass)
	{
		if (isRealTable(enumClass)) throw new RuntimeException("[Deliver]Table表无法获取到视图JOIN关系");
		Map<String, List<String>> result = new HashMap<>();
		LeftJoin[] leftJoins = enumClass.getAnnotationsByType(LeftJoin.class);
		for (LeftJoin leftJoin : leftJoins)
		{
			String key = getRealTableName(leftJoin.tb());
			List<String> list = result.get(key);
			if (list == null)
			{
				list = new ArrayList<>();
				result.put(key, list);
			}
			list.addAll(Arrays.asList(leftJoin.on()));
		}
		return result;
	}



	public static <T extends Enum> Map<T, String> getADBTFieldMap(Class<T> enumClass)
	{
		// 自动填充ADBT元数据
		if (isRealTable(enumClass)) inflateTableMetedata(enumClass);
		// 通过反射读取到所有的定义字段
		Map<T, String> declareFieldMap = new LinkedHashMap<>();
		for (T current : enumClass.getEnumConstants())
		{
			try
			{
				Field field = enumClass.getField(current.name());
				if (field.isAnnotationPresent(From.class)) declareFieldMap.put(current, field.getAnnotation(From.class).value());
				else declareFieldMap.put(current, current.name());
			} catch (NoSuchFieldException e)
			{
				throw new RuntimeException(e);
			}
		}
		return declareFieldMap;
	}

	public static <T extends Enum> Set<T> getADBTPrimarySet(Class<T> enumClass)
	{
		// 获取到表明和当前定义实际的主键，对于视图则直接返回空集合
		if (isRealTable(enumClass)) inflateTableMetedata(enumClass);
		else return Collections.emptySet();//视图没有主键的概念
		// 通过反射读取到定义的主键
		Set<T> declarePrimaryKeySet = new LinkedHashSet<>();
		for (T current : enumClass.getEnumConstants())
		{
			try
			{
				Field field = enumClass.getField(current.name());
				if (field.isAnnotationPresent(PrimaryKey.class)) declarePrimaryKeySet.add(current);
			} catch (NoSuchFieldException e)
			{
				throw new RuntimeException(e);
			}
		}
		return declarePrimaryKeySet;
	}


}
