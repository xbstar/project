package com.xbstar.types;

import java.io.Serializable;

public class Column implements Serializable
{
	public String table;
	public String field;
	public Type type;
	public Boolean primary;
	public Boolean notnull;
	public Boolean visual; //是否为虚拟字段MySQL8.0以后有此特性
	public String comment; //字段注释，SQLite无法设置但MySQL可以有
	public String columnIndex; //主键保存为PRI:PRIMARY，唯一索引保存为UNI:索引名  普通索引保存为MUL:索引名
	public String defaultValue;

	@Override
	public String toString()
	{
		return "Column{" + "table=" + table + "\tprimary=" + primary + "\tnotnull=" + notnull + "\tdefault=" + defaultValue + "\ttype=" + type + "\tfield=" + field + '}';
	}
}
