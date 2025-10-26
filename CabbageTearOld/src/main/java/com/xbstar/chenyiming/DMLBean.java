package com.xbstar.chenyiming;

import java.util.Map;

public class DMLBean
{
	private String op;
	private int size;
	private Map<String, String> keys;
	private long time;
	private Map<String, String> after;
	private Map<String, String> before;
	private String db;
	private String table;

	public String getOp()
	{
		return op;
	}

	public void setOp(String op)
	{
		this.op = op;
	}

	public int getSize()
	{
		return size;
	}

	public void setSize(int size)
	{
		this.size = size;
	}

	public Map<String, String> getKeys()
	{
		return keys;
	}

	public void setKeys(Map<String, String> keys)
	{
		this.keys = keys;
	}

	public long getTime()
	{
		return time;
	}

	public void setTime(long time)
	{
		this.time = time;
	}

	public Map<String, String> getAfter()
	{
		return after;
	}

	public void setAfter(Map<String, String> after)
	{
		this.after = after;
	}

	public Map<String, String> getBefore()
	{
		return before;
	}

	public void setBefore(Map<String, String> before)
	{
		this.before = before;
	}

	public String getDb()
	{
		return db;
	}

	public void setDb(String db)
	{
		this.db = db;
	}

	public String getTable()
	{
		return table;
	}

	public void setTable(String table)
	{
		this.table = table;
	}

	@Override
	public String toString()
	{
		return "DMLBean{" +
				"op='" + op + '\'' +
				", size=" + size +
				", keys=" + keys +
				", time=" + time +
				", after=" + after +
				", before=" + before +
				", db='" + db + '\'' +
				", table='" + table + '\'' +
				'}';
	}
}
