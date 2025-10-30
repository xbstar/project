package com.xbstar.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UDLUtils
{
	public static Pattern ForbidFieldEmpty = Pattern.compile("^(?<level>[^@]+)@(?<cluster>[^:_]+)_(?<type>[^:/]+)://(?<database>[^/]+)/(?<table>[^/]+)/(?<field>[^/]+)$");
	public static Pattern AllowFieldEmpty = Pattern.compile("^(?<level>[^@]+)@(?<cluster>[^:_]+)_(?<type>[^:/]+)://(?<database>[^/]+)/(?<table>[^/]+)(?:/(?<field>[^/]+))?$");
	public static Pattern AllowTableEmpty = Pattern.compile("^(?<level>[^@]+)@(?<cluster>[^:_]+)_(?<type>[^:/]+)://(?<database>[^/]+)(?:/(?<table>[^/]+)?(?:/(?<field>[^/]+))?)?$");

	public static enum Type
	{
		DatabaseUDL,
		TableUDL,
		FieldUDL
	}

	public static void main(String[] args)
	{
		String udl = "ODS@xbstar-clu1_mysql://xbstar-student/stud/age";
		System.out.println(getLevel(udl));
		System.out.println(getCluster(udl));
		System.out.println(getType(udl));
		System.out.println(getDatabase(udl));
		System.out.println(getTable(udl));
		System.out.println(getField(udl));
	}

	private static Matcher getMatcher(String udl, Pattern pattern)
	{
		Matcher matcher = pattern.matcher(udl);
		if (!matcher.find()) throw new RuntimeException(udl + "不符合UDL语法格式:level@cluster_type://database/table/column");
		return matcher;
	}

	public static Type type(String udl)
	{
		if (ForbidFieldEmpty.matcher(udl).find()) return Type.FieldUDL;
		if (AllowFieldEmpty.matcher(udl).find()) return Type.TableUDL;
		if (AllowTableEmpty.matcher(udl).find()) return Type.DatabaseUDL;
		throw new RuntimeException(udl + "不符合UDL语法格式:level@cluster_type://database/table/column");
	}

	public static boolean checkUDL(String udl, Pattern pattern)
	{
		Matcher matcher = pattern.matcher(udl);
		return matcher.find();
	}

	public static String makeDatabaseUDL(Object level, Object cluster, Object type, Object database)
	{
		return level + "@" + cluster + "_" + type + "://" + database;
	}

	public static String makeTableUDL(Object level, Object cluster, Object type, Object database, Object table)
	{
		return level + "@" + cluster + "_" + type + "://" + database + "/" + table;
	}

	public static String getDatabaseUDL(String udl)
	{
		Matcher matcher = getMatcher(udl, AllowTableEmpty);
		return matcher.group("level") + "@" + matcher.group("cluster") + "_" + matcher.group("type") + "://" + matcher.group("database");
	}

	public static String getTableUDL(String udl)
	{
		Matcher matcher = getMatcher(udl, AllowFieldEmpty);
		return matcher.group("level") + "@" + matcher.group("cluster") + "_" + matcher.group("type") + "://" + matcher.group("database") + "/" + matcher.group("table");
	}

	public static String getLevel(String udl)
	{
		return getMatcher(udl, AllowTableEmpty).group("level");
	}

	public static String getCluster(String udl)
	{
		return getMatcher(udl, AllowTableEmpty).group("cluster");
	}

	public static String getType(String udl) {return getMatcher(udl, AllowTableEmpty).group("type");}

	public static String getDatabase(String udl)
	{
		return getMatcher(udl, AllowTableEmpty).group("database");
	}

	public static String getTable(String udl)
	{
		return getMatcher(udl, AllowFieldEmpty).group("table");
	}

	public static String getField(String udl)
	{
		return getMatcher(udl, ForbidFieldEmpty).group("field");
	}

	/** 将非法的字符转化为合法的名称*/
	public static String getValidDirName(String udl)
	{
		if (udl == null) return "NAME_IS_NULL";
		String result = udl.replace("@", "-");
		result = result.replace("://", ".");
		result = result.replace("/", ".");
		return result;
	}

	/** 将非法的字符转化为合法的Pod名称*/
	public static String getValidPodName(String udl)
	{
		if (udl == null) throw new RuntimeException("name不能为空");
		String result = udl.replace("@", "-");
		result = result.replace("://", ".");
		result = result.replace("/", ".");
		result = result.replace("_", "-");
		return result.toLowerCase();
	}
}
