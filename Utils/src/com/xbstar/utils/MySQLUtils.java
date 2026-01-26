package com.xbstar.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
//import org.springframework.http.HttpStatus;
//import org.springframework.http.ResponseEntity;

import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MySQLUtils
{
	public static Pattern JDBCPattern = Pattern.compile("^jdbc:mysql://(?<host>[^:/]+)(?::(?<port>\\d+))?/(?<database>[^/?]+)");
	// 字段操作正则表达式
	public static Pattern ADDColumnPattern = Pattern.compile("^\\s*ADD\\s+(?:COLUMN\\s+)?`?([\\w_]+)`?\\s+(.+)$", Pattern.CASE_INSENSITIVE);
	public static Pattern DROPColumnPattern = Pattern.compile("^\\s*DROP\\s+(?:COLUMN\\s+)?`?([\\w_]+)`?\\s*$", Pattern.CASE_INSENSITIVE);
	public static Pattern MODIFYColumnPattern = Pattern.compile("^\\s*MODIFY\\s+(?:COLUMN\\s+)?`?([\\w_]+)`?\\s+(.+)$", Pattern.CASE_INSENSITIVE);
	public static Pattern CHANGEColumnPattern = Pattern.compile("^\\s*CHANGE\\s+(?:COLUMN\\s+)?`?([\\w_]+)`?\\s+`?([\\w_]+)`?\\s+(.+)$", Pattern.CASE_INSENSITIVE);
	public static Pattern RENAMEColumnPattern = Pattern.compile("^\\s*RENAME\\s+(?:COLUMN\\s+)?`?([\\w_]+)`?\\s+TO\\s+`?([\\w_]+)`?\\s*$", Pattern.CASE_INSENSITIVE);
	// 约束操作正则表达式
	public static Pattern ADDPrimaryPattern = Pattern.compile("^\\s*ADD\\s+PRIMARY\\s+KEY\\s*\\(\\s*([^)]+)\\s*\\)(?:\\s+USING\\s+\\w+)?\\s*$", Pattern.CASE_INSENSITIVE);
	public static Pattern DROPPrimaryPattern = Pattern.compile("^\\s*DROP\\s+PRIMARY\\s+KEY\\s*$", Pattern.CASE_INSENSITIVE);
	public static Pattern ADDUniquePattern = Pattern.compile("^\\s*ADD\\s+UNIQUE\\s+(?:INDEX\\s+|KEY\\s+)?`?([\\w_]+)`?\\s*\\(\\s*`?([\\w_]+)`?(?:\\s*,\\s*`?[\\w_]+`?)*\\s*\\)\\s*(?:USING\\s+\\w+)?\\s*$", Pattern.CASE_INSENSITIVE);
	public static Pattern DROPUniquePattern = Pattern.compile("^\\s*DROP\\s+UNIQUE\\s+(?:INDEX\\s+|KEY\\s+)?`?([\\w_]+)`?\\s*$", Pattern.CASE_INSENSITIVE);
	public static Pattern ADDConstraintPattern = Pattern.compile("^\\s*ADD\\s+CONSTRAINT\\s+`?([\\w_]+)`?\\s+.+$", Pattern.CASE_INSENSITIVE);
	public static Pattern DROPConstraintPattern = Pattern.compile("^\\s*DROP\\s+CONSTRAINT\\s+`?([\\w_]+)`?\\s*$", Pattern.CASE_INSENSITIVE);
	public static Pattern ADDForeignKeyPattern = Pattern.compile("^\\s*ADD\\s+FOREIGN\\s+KEY\\s*\\((.+?)\\)\\s+REFERENCES\\s+`?([\\w_]+)`?\\s*\\((.+?)\\)(?:\\s+ON\\s+\\w+\\s+\\w+)*\\s*$", Pattern.CASE_INSENSITIVE);
	public static Pattern DROPForeignKeyPattern = Pattern.compile("^\\s*DROP\\s+FOREIGN\\s+KEY\\s+`?([\\w_]+)`?\\s*$", Pattern.CASE_INSENSITIVE);
	// 索引操作正则表达式
	public static Pattern AUTOINCREMENTPattern = Pattern.compile("`([^`]+)`\\s+[^,]+?AUTO_INCREMENT", Pattern.CASE_INSENSITIVE);
	public static Pattern ALLINDEXINDDLPattern = Pattern.compile("((UNIQUE\\s+)?(KEY|INDEX)|FULLTEXT\\s+(KEY|INDEX)|SPATIAL\\s+(KEY|INDEX)|CONSTRAINT)\\s*.*", Pattern.CASE_INSENSITIVE);
	public static Pattern ADDFULLTEXTPattern = Pattern.compile("^\\s*ADD\\s+FULLTEXT\\s+(?:INDEX\\s+|KEY\\s+)?`?([\\w_]+)`?\\s*\\((.+?)\\)(?:\\s+WITH\\s+PARSER\\s+([\\w_]+))?\\s*$", Pattern.CASE_INSENSITIVE);
	public static Pattern ADDIndexPattern = Pattern.compile("^\\s*ADD\\s+(?:INDEX\\s+|KEY\\s+)?`?([\\w_]+)`?\\s*\\(\\s*(.+?)\\s*\\)(?:\\s+USING\\s+(\\w+))?\\s*$", Pattern.CASE_INSENSITIVE);
	public static Pattern DROPIndexPattern = Pattern.compile("^\\s*DROP\\s+(?:INDEX\\s+|KEY\\s+)?`?([\\w_]+)`?\\s*$", Pattern.CASE_INSENSITIVE);
	public static Pattern RENAMEIndexPattern = Pattern.compile("^\\s*RENAME\\s+(?:INDEX\\s+|KEY\\s+)?`?([\\w_]+)`?\\s+TO\\s+`?([\\w_]+)`?\\s*$", Pattern.CASE_INSENSITIVE);

	public static String getHost(String jdbc)
	{
		Matcher matcher = JDBCPattern.matcher(jdbc);
		if (!matcher.find()) throw new RuntimeException(jdbc + "不符合JDBC语法格式:jdbc:mysql://host:port/database");
		return matcher.group("host");
	}

	public static String getHost(Connection connection)
	{
		return getHost(getJDBCAddress(connection));
	}

	public static Integer getPort(String jdbc)
	{
		Matcher matcher = JDBCPattern.matcher(jdbc);
		if (!matcher.find()) throw new RuntimeException(jdbc + "不符合JDBC语法格式:jdbc:mysql://host:port/database");
		return Integer.valueOf(matcher.group("port"));
	}

	public static Integer getPort(Connection connection)
	{
		return getPort(getJDBCAddress(connection));
	}

	public static String getDatabase(String jdbc)
	{
		Matcher matcher = JDBCPattern.matcher(jdbc);
		if (!matcher.find()) throw new RuntimeException(jdbc + "不符合JDBC语法格式:jdbc:mysql://host:port/database");
		return matcher.group("database");
	}

	/** 从连接中获取连接数据库信息*/
	public static String getDatabase(Connection connection)
	{
		try
		{
			String database = connection.getCatalog();
			if (database == null) throw new RuntimeException("Connection中没有包含连接数据库信息");
			return database;
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public static String getDataBaseVersion(Connection connection)
	{
		try
		{
			ResultSet resultSet = connection.createStatement().executeQuery("SELECT VERSION()");
			resultSet.next();
			String version = resultSet.getString(1);
			version = version.contains("-") ? version.substring(0, version.indexOf("-")) : version;
			resultSet.close();
			return version;
		} catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	//	public static String getDataBaseVersion(String jdbcURL, String userName, String passWord)
	//	{
	//		Pattern pattern = Pattern.compile("jdbc:mysql://([^:/]+)(?::(\\d+))?/([^?]+)");
	//		Matcher matcher = pattern.matcher(jdbcURL);
	//		if (matcher.find())
	//		{
	//			String host = matcher.group(1);
	//			String port = matcher.group(2) != null ? matcher.group(2) : "3306";
	//			String db = matcher.group(3);
	//			ResponseEntity<String> res = MySQLUtils.checkMySQLConnection(host, Integer.valueOf(port), userName, passWord, null);
	//			if (res.getStatusCode() == HttpStatus.OK)
	//			{
	//				return res.getBody();
	//			}
	//			else
	//			{
	//				throw new RuntimeException(jdbcURL + "无法连接,异常是:" + res.getBody());
	//			}
	//		}
	//		else
	//		{
	//			throw new RuntimeException("JDBC地址(" + jdbcURL + ")无效!");
	//		}
	//	}

	/* 获取到连接的连接JDBC地址*/
	public static String getJDBCAddress(Connection jdbcConnection)
	{
		try
		{
			String jdbcAddress = jdbcConnection.getMetaData().getURL();
			return jdbcAddress;
		} catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/* 获取MySQL连接的监控ID*/
	public static long getConnectionID(Connection jdbcConnection)
	{
		try
		{
			ResultSet cursor = jdbcConnection.createStatement().executeQuery("SELECT CONNECTION_ID()");
			cursor.next();
			long result = cursor.getLong(1);
			cursor.close();
			return result;
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	/** 获取所有连接到MySQL的链接的ID集合*/
	public static Set<Long> getConnectionIDSet(Connection jdbcConnection)
	{
		try
		{
			Set<Long> result = new HashSet<>();
			ResultSet cursor = jdbcConnection.createStatement().executeQuery("SHOW PROCESSLIST");
			while (cursor.next()) result.add(cursor.getLong("Id"));
			cursor.close();
			return result;
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public static Boolean getIsTableIdCaseSensitive(Connection connection)
	{
		try
		{
			ResultSet cursor = connection.createStatement().executeQuery("SHOW VARIABLES LIKE 'lower_case_table_names'");
			cursor.next();
			boolean result = !"0".equals(cursor.getString(2));
			cursor.close();
			return result;
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public static List<String> getBinlogList(Connection jdbcConnection)
	{
		try
		{
			List<String> result = new ArrayList<>();
			ResultSet cursor = jdbcConnection.createStatement().executeQuery("show binary logs");
			while (cursor.next()) result.add(cursor.getString(1));
			cursor.close();
			return result;
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public static Map.Entry<String, Long> getMasterBinlog(Connection jdbcConnection)
	{
		try
		{
			ResultSet cursor = jdbcConnection.createStatement().executeQuery("SHOW MASTER STATUS");
			if (!cursor.next()) throw new RuntimeException("binlog可能没有开启");
			Map.Entry<String, Long> result = Map.entry(cursor.getString("File"), cursor.getLong("Position"));
			cursor.close();
			return result;
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	/* 判断指定的表是否在数据库中存在*/
	public static boolean getTableExist(Connection jdbcConnection, String table)
	{
		try
		{
			String database = jdbcConnection.getCatalog();
			if (table == null || "".equals(table)) throw new RuntimeException("没有传入表");
			if (database == null || "".equals(database.trim())) throw new RuntimeException("没有传入库");
			String sql = "SELECT TABLE_NAME FROM information_schema.tables " + //
					"WHERE TABLE_TYPE = 'BASE TABLE' " + //
					"AND TABLE_SCHEMA = '" + database + "' " +//
					"AND TABLE_NAME = '" + table + "'";
			ResultSet tableCursor = jdbcConnection.createStatement().executeQuery(sql);
			boolean result = tableCursor.next();
			tableCursor.close();
			return result;
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	/** 判断表是否为空，1条数据也没有返回true，空表*/
	public static Boolean getTableEmpty(Connection jdbcConnection, String table)
	{
		try
		{
			ResultSet cursor = jdbcConnection.createStatement().executeQuery("SELECT * FROM `" + table + "` LIMIT 1");
			Boolean result = !cursor.next();
			cursor.close();
			return result;
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	/** 查询给定tables集合的表记录数统计信息，如果table为null，则给整个数据库的*/
	public static List<Map.Entry<String, Long>> getTableStatics(Connection jdbcConnection, Set<String> tables)
	{
		try
		{
			String database = jdbcConnection.getCatalog();
			if (database == null) throw new RuntimeException("必须选定数据库");
			StringBuilder sqlBuilder = new StringBuilder();
			sqlBuilder.append("SELECT table_name,table_rows FROM information_schema.tables WHERE table_schema='" + database + "' ");
			if (tables != null && tables.size() > 0)
			{
				sqlBuilder.append("AND table_name in (");
				Iterator<String> iterator = tables.iterator();
				while (iterator.hasNext())
				{
					sqlBuilder.append("'" + iterator.next() + "'");
					if (iterator.hasNext()) sqlBuilder.append(",");
				}
				sqlBuilder.append(") ");
			}
			sqlBuilder.append("ORDER BY data_length ASC");
			ResultSet cursor = jdbcConnection.createStatement().executeQuery(sqlBuilder.toString());
			List<Map.Entry<String, Long>> result = new ArrayList<>();
			while (cursor.next())
			{
				result.add(Map.entry(cursor.getString("table_name"), cursor.getLong("table_rows")));
			}
			return result;
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	/** 从MySQL的JDBC链接中获得所有数据表的集合*/
	public static TreeSet<String> getTableSet(Connection jdbcConnection)
	{
		try
		{
			String database = jdbcConnection.getCatalog();
			if (database == null || "".equals(database.trim())) throw new RuntimeException("没有传入库");
			String sql = "SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '" + database + "'";
			ResultSet tableCursor = jdbcConnection.createStatement().executeQuery(sql);
			TreeSet<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
			while (tableCursor.next())
			{
				String tableName = tableCursor.getString("TABLE_NAME");
				result.add(tableName);
			}
			tableCursor.close();
			return result;
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}


	/** 获取limit个数据库中的表信息*/
	public static List<JSONObject> getTableInfo(Connection connection, String filter, Integer limit)
	{
		try
		{
			String database = connection.getCatalog();
			if (database == null || "".equals(database.trim())) throw new RuntimeException("没有传入库");
			String sql = "SELECT TABLE_NAME,TABLE_COMMENT,TABLE_ROWS FROM information_schema.tables WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '" + database + "'";
			if (filter != null && !"".equals(filter.trim())) sql = sql + " AND TABLE_NAME like '%" + filter + "%'";
			if (limit != null) sql = sql + " LIMIT " + limit;
			ResultSet tableCursor = connection.createStatement().executeQuery(sql);
			List<JSONObject> result = new ArrayList<>();
			while (tableCursor.next())
			{
				JSONObject tableJSON = new JSONObject();
				tableJSON.put("name", tableCursor.getString("TABLE_NAME"));
				tableJSON.put("comment", tableCursor.getString("TABLE_COMMENT"));
				tableJSON.put("count", tableCursor.getLong("TABLE_ROWS"));
				result.add(tableJSON);
			}
			tableCursor.close();
			return result;
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public static Set<String> getTableIndexNameSet(Connection jdbcConnection, String table)
	{
		try
		{
			String database = jdbcConnection.getCatalog();
			if (database == null || "".equals(database.trim())) throw new RuntimeException("没有传入库");
			if (table == null || "".equals(table)) throw new RuntimeException("没有传入表");
			String sql = "SHOW INDEX FROM `" + table + "`";
			ResultSet cursor = jdbcConnection.createStatement().executeQuery(sql);
			Set<String> result = new HashSet<>();
			while (cursor.next()) result.add(cursor.getString("Key_name"));
			return result;
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	/** 查询指定数据表的唯一索引情况，只返回1个<COLUMN_NAME,UNIQUE,INDEX_NAME<的键值对
	 *  如果存在多个索引，按照INDEX_NAME = PRIMARY > 唯一索引名  的顺序
	 *  如果没有任何的索引，或者只有普通索引，则返回null
	 */
	public static Map.Entry<String, String> getTableUniqueIndex(Connection jdbcConnection, String table) throws SQLException
	{
		String database = jdbcConnection.getCatalog();
		if (database == null || "".equals(database.trim())) throw new RuntimeException("没有传入库");
		if (table == null || "".equals(table)) throw new RuntimeException("没有传入表");
		String sql = "SELECT ANY_VALUE(COLUMN_NAME) as COLUMN_NAME, ANY_VALUE(INDEX_NAME) as INDEX_NAME " //
				+ "FROM INFORMATION_SCHEMA.STATISTICS " //
				+ "WHERE TABLE_SCHEMA = '" + database + "' "//
				+ "AND "//
				+ "TABLE_NAME = '" + table + "' " //
				+ "AND " //
				+ "(INDEX_NAME = 'PRIMARY' " //优先选择主键Primary
				+ "OR " //
				+ "NON_UNIQUE = 0) "// 如果没主键唯一键也不错
				+ "GROUP BY TABLE_NAME;";
		ResultSet cursor = jdbcConnection.createStatement().executeQuery(sql);
		Map.Entry<String, String> result = null;
		if (cursor.next())
		{
			//注意取下反把NON_UNIQUE变成UNIQUE
			result = new AbstractMap.SimpleEntry<>(cursor.getString(1), cursor.getString(2));
		}
		else
		{
			//这张表没有任何的索引
		}
		cursor.close();
		return result;
	}

	public static long getTableRecordCount(Connection jdbcConnection, String table)
	{
		try
		{
			String database = jdbcConnection.getCatalog();
			if (database == null) throw new RuntimeException("没有传入库");
			if (table == null) throw new RuntimeException("没有传入表");
			String column = getTableUniqueIndex(jdbcConnection, table).getKey();
			ResultSet cursor = jdbcConnection.createStatement().executeQuery("SELECT COUNT(" + column + ") FROM " + table);
			cursor.next();
			long result = cursor.getLong(1);
			cursor.close();
			return result;
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}

	}



	/** 根虎表名获取表的DDL语句*/
	public static String getTableDDL(Connection jdbcConnection, String table)
	{
		try
		{
			ResultSet cursor = jdbcConnection.createStatement().executeQuery("SHOW CREATE TABLE `" + table + "`");
			cursor.next();
			String createTableDDL = cursor.getString("Create Table");
			cursor.close();
			return createTableDDL;
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	/** 根据表名获取表的注释*/
	public static String getTableComment(Connection connection, String table)
	{
		try
		{
			String database = connection.getCatalog();
			if (database == null || "".equals(database.trim())) throw new RuntimeException("没有传入库");
			if (table == null || "".equals(table)) throw new RuntimeException("没有传入表");
			ResultSet cursor = connection.createStatement().executeQuery("SELECT TABLE_COMMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + database + "' AND TABLE_NAME = '" + table + "'");
			String comment = "";
			if (cursor.next()) comment = cursor.getString(1);
			else PrintUtils.printError("发现错误的表获取不到注释Cursor:" + table);
			cursor.close();
			return comment;
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	/** 根据表名获取表的所有字段*/
	public static JSONArray getTableColumns(Connection connection, String table)
	{
		try
		{
			String database = connection.getCatalog();
			if (database == null || "".equals(database.trim())) throw new RuntimeException("没有传入库");
			if (table == null || "".equals(table)) throw new RuntimeException("没有传入表");
			String sql = "SELECT\n" //
					+ "\t`COLUMNS`.COLUMN_NAME, \n" //
					+ "\t`COLUMNS`.EXTRA, \n" //
					+ "\t`COLUMNS`.IS_NULLABLE, \n" //
					+ "\t`COLUMNS`.DATA_TYPE, \n" //
					+ "\t`COLUMNS`.CHARACTER_MAXIMUM_LENGTH, \n" //
					+ "\t`COLUMNS`.NUMERIC_PRECISION, \n" //
					+ "\t`COLUMNS`.NUMERIC_SCALE, \n" //
					+ "\t`COLUMNS`.ORDINAL_POSITION, \n" //
					+ "\t`COLUMNS`.COLUMN_KEY, \n" //
					+ "\t`COLUMNS`.COLUMN_COMMENT\n"//
					+ "FROM\n" //
					+ "\tinformation_schema.`COLUMNS`\n" //
					+ "WHERE\n" //
					+ "\t`COLUMNS`.TABLE_SCHEMA = '" + database + "' AND\n" //
					+ "\t`COLUMNS`.TABLE_NAME = '" + table + "'\n" //
					+ "ORDER BY ORDINAL_POSITION asc";
			ResultSet cursor = connection.createStatement().executeQuery(sql);
			// 开始构建返回结构
			JSONArray result = new JSONArray();
			while (cursor.next())
			{
				JSONObject column = new JSONObject();
				String columnName = cursor.getString("COLUMN_NAME");
				column.put("table", table);
				column.put("name", columnName);
				column.put("nullable", cursor.getString("IS_NULLABLE"));
				column.put("type", cursor.getString("DATA_TYPE"));
				Long length = cursor.getLong("CHARACTER_MAXIMUM_LENGTH");
				column.put("length", length == null ? null : (int) (long) length);
				column.put("precision", cursor.getInt("NUMERIC_PRECISION"));
				column.put("scale", cursor.getInt("NUMERIC_SCALE"));
				column.put("sort", cursor.getInt("ORDINAL_POSITION"));
				String key = cursor.getString("COLUMN_KEY");//有PRI,MUL,UNI三种可能
				if ("".equals(key)) column.put("key", "");
				else
				{
					ResultSet keyCur = connection.createStatement().executeQuery("SHOW INDEX FROM `" + table + "` WHERE Column_name = '" + columnName + "'");
					if (!keyCur.next()) throw new RuntimeException(table + "表中没有找到字段" + column + "的索引");
					String keyName = keyCur.getString("Key_name");
					keyCur.close();
					column.put("key", key + ":" + keyName);
				}
				column.put("visual", "VIRTUAL GENERATED".equals(cursor.getString("EXTRA")) || "STORED GENERATED".equals(cursor.getString("EXTRA")));
				column.put("comment", cursor.getString("COLUMN_COMMENT"));
				result.add(column);
			}
			cursor.close();
			return result;
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public static JSONObject getTableColumn(Connection connection, String table, String column)
	{
		try
		{
			String database = connection.getCatalog();
			if (database == null) throw new RuntimeException("没有传入库");
			if (table == null) throw new RuntimeException("没有传入表");
			if (column == null) throw new RuntimeException("没有传入列");
			String sql = "SELECT\n" //
					+ "\t`COLUMNS`.COLUMN_NAME, \n" //
					+ "\t`COLUMNS`.IS_NULLABLE, \n" //
					+ "\t`COLUMNS`.DATA_TYPE, \n" //
					+ "\t`COLUMNS`.CHARACTER_MAXIMUM_LENGTH, \n" //
					+ "\t`COLUMNS`.NUMERIC_PRECISION, \n" //
					+ "\t`COLUMNS`.NUMERIC_SCALE, \n" //
					+ "\t`COLUMNS`.COLUMN_KEY, \n" //
					+ "\t`COLUMNS`.COLUMN_COMMENT\n"//
					+ "FROM\n" //
					+ "\tinformation_schema.`COLUMNS`\n" //
					+ "WHERE\n" //
					+ "\t`COLUMNS`.TABLE_SCHEMA = '" + database + "' AND\n" //
					+ "\t`COLUMNS`.COLUMN_NAME = '" + column + "' AND\n" //
					+ "\t`COLUMNS`.TABLE_NAME = '" + table + "'";
			ResultSet cursor = connection.createStatement().executeQuery(sql);
			// 开始构建返回结构
			JSONObject result = new JSONObject();
			result.put("table", table);
			if (cursor.next())
			{
				result.put("name", cursor.getString("COLUMN_NAME"));
				result.put("nullable", cursor.getString("IS_NULLABLE"));
				result.put("type", cursor.getString("DATA_TYPE"));
				Long length = cursor.getLong("CHARACTER_MAXIMUM_LENGTH");
				result.put("length", length == null ? null : (int) (long) length);
				result.put("precision", cursor.getInt("NUMERIC_PRECISION"));
				result.put("scale", cursor.getInt("NUMERIC_SCALE"));
				String key = cursor.getString("COLUMN_KEY");//有PRI,MUL,UNI三种可能
				if ("".equals(key)) result.put("key", "");
				else
				{
					ResultSet keyCur = connection.createStatement().executeQuery("SHOW INDEX FROM `" + table + "` WHERE Column_name = '" + column + "'");
					if (!keyCur.next()) throw new RuntimeException(table + "表中没有找到字段" + column + "的索引");
					String keyName = keyCur.getString("Key_name");
					keyCur.close();
					result.put("key", key + ":" + keyName);
				}
				result.put("comment", cursor.getString("COLUMN_COMMENT"));
			}
			else
			{
				result = null;
				PrintUtils.printError("表" + table + "中不存在字段" + column);
			}
			cursor.close();
			return result;
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public static JSONArray getTableColumnPK(Connection connection, String table)
	{
		try
		{
			String database = connection.getCatalog();
			if (database == null) throw new RuntimeException("没有传入库");
			if (table == null) throw new RuntimeException("没有传入表");
			String sql = "SELECT\n" //
					+ "\t`COLUMNS`.COLUMN_NAME, \n" //
					+ "\t`COLUMNS`.IS_NULLABLE, \n" //
					+ "\t`COLUMNS`.DATA_TYPE, \n" //
					+ "\t`COLUMNS`.CHARACTER_MAXIMUM_LENGTH, \n" //
					+ "\t`COLUMNS`.NUMERIC_PRECISION, \n" //
					+ "\t`COLUMNS`.NUMERIC_SCALE, \n" //
					+ "\t`COLUMNS`.COLUMN_KEY, \n" //
					+ "\t`COLUMNS`.COLUMN_COMMENT\n"//
					+ "FROM\n" //
					+ "\tinformation_schema.`COLUMNS`\n" //
					+ "WHERE\n" //
					+ "\t`COLUMNS`.TABLE_SCHEMA = '" + database + "' AND\n" //
					+ "\t`COLUMNS`.COLUMN_KEY = 'PRI' AND\n" //
					+ "\t`COLUMNS`.TABLE_NAME = '" + table + "'";
			ResultSet cursor = connection.createStatement().executeQuery(sql);
			// 开始构建返回结构
			JSONArray result = new JSONArray();
			while (cursor.next())
			{
				JSONObject column = new JSONObject();
				column.put("name", cursor.getString("COLUMN_NAME"));
				column.put("nullable", cursor.getString("IS_NULLABLE"));
				column.put("type", cursor.getString("DATA_TYPE"));
				column.put("length", cursor.getString("CHARACTER_MAXIMUM_LENGTH"));
				column.put("precision", cursor.getString("NUMERIC_PRECISION"));
				column.put("scale", cursor.getString("NUMERIC_SCALE"));
				column.put("key", cursor.getString("COLUMN_KEY"));
				column.put("comment", cursor.getString("COLUMN_COMMENT"));
				result.add(column);
			}
			cursor.close();
			return result;
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public static String getColumnIndexName(Connection jdbcConnection, String table, String column) throws SQLException
	{
		if (jdbcConnection.getCatalog() == null) throw new RuntimeException("传入的链接没有选择数据库");
		ResultSet cursor = jdbcConnection.createStatement().executeQuery("SHOW INDEX FROM `" + table + "` WHERE Column_name = '" + column + "'");
		if (!cursor.next()) throw new RuntimeException(table + "表中没有找到字段" + column + "的索引");
		String result = cursor.getString("Key_name");
		cursor.close();
		return result;
	}

	public static Long getTableSplitCount(Connection jdbcConnection, String table, Object[] start, Object[] end)
	{
		try
		{
			StringBuilder sqlBuilder = new StringBuilder();
			Set<String> tablePrimaryKey = JDBCUtils.getTablePrimaryKey(jdbcConnection, table);
			sqlBuilder.append("SELECT COUNT(*) FROM `" + table + "` ");
			if (start != null || end != null) sqlBuilder.append(" WHERE ");
			if (start != null)
			{
				Iterator<String> pkit = tablePrimaryKey.iterator();
				int index = 0;
				while (pkit.hasNext())
				{
					if (start.length <= index) break;
					if (index > 0) sqlBuilder.append(" AND ");
					String key = pkit.next();
					String val = start[index] instanceof String ? "'" + start[index] + "'" : start[index].toString();
					sqlBuilder.append(key + " >= " + val);
					index++;
				}
			}
			if (end != null)
			{
				Iterator<String> pkit = tablePrimaryKey.iterator();
				if (start != null) sqlBuilder.append(" AND ");
				int index = 0;
				while (pkit.hasNext())
				{
					if (end.length <= index) break;
					if (index > 0) sqlBuilder.append(" AND ");
					String key = pkit.next();
					String val = end[index] instanceof String ? "'" + end[index] + "'" : end[index].toString();
					sqlBuilder.append(key + " < " + val);
					index++;
				}
			}
			Statement statement = jdbcConnection.createStatement();
			statement.setQueryTimeout(10);
			ResultSet cursor = statement.executeQuery(sqlBuilder.toString());
			cursor.next();
			long result = cursor.getLong(1);
			cursor.close();
			statement.close();
			return result;
		} catch (SQLTimeoutException e)
		{
			return null;
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public static Long getDistinctColumnCount(Connection jdbcConnection, String table, String column)
	{
		try
		{
			ResultSet disCursor = jdbcConnection.createStatement().executeQuery("SELECT  COUNT(DISTINCT `" + column + "`) FROM `" + table + "`");
			disCursor.next();
			Long dis = disCursor.getLong(1);
			disCursor.close();
			return dis;
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public static Long getHasValueColumnCount(Connection jdbcConnection, String table, String column)
	{
		try
		{
			ResultSet hasCursor = jdbcConnection.createStatement().executeQuery("SELECT  COUNT(`" + column + "`) FROM `" + table + "`");
			hasCursor.next();
			Long has = hasCursor.getLong(1);
			hasCursor.close();
			return has;
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public static void dropTableForeignKey(Connection jdbcConnection, String table) throws SQLException
	{
		String database = jdbcConnection.getCatalog();
		if (database == null || "".equals(database.trim())) throw new RuntimeException("没有传入库");
		if (table == null || "".equals(table)) throw new RuntimeException("没有传入表");
		String sql = "SELECT DISTINCT " //
				+ "KEY_COLUMN_USAGE.CONSTRAINT_NAME " //
				+ "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "//
				+ "JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS " //
				+ "ON KEY_COLUMN_USAGE.CONSTRAINT_NAME = TABLE_CONSTRAINTS.CONSTRAINT_NAME "//
				+ "AND KEY_COLUMN_USAGE.TABLE_SCHEMA = TABLE_CONSTRAINTS.TABLE_SCHEMA "//
				+ "AND KEY_COLUMN_USAGE.TABLE_NAME = TABLE_CONSTRAINTS.TABLE_NAME "//
				+ "WHERE KEY_COLUMN_USAGE.TABLE_SCHEMA = '" + database + "' "//
				+ "AND KEY_COLUMN_USAGE.TABLE_NAME = '" + table + "' " //
				+ "AND CONSTRAINT_TYPE = 'FOREIGN KEY'";
		ResultSet rs = jdbcConnection.createStatement().executeQuery(sql);
		while (rs.next())
		{
			String constraintName = rs.getString("CONSTRAINT_NAME");
			String deleteSql = "ALTER TABLE " + table + " DROP FOREIGN KEY " + constraintName;
			PrintUtils.printWarning(deleteSql);
			jdbcConnection.createStatement().execute(deleteSql);
		}
		rs.close();
	}
}
