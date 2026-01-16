package com.xbstar.utils;

import com.xbstar.http.HttpStatus;
import com.xbstar.http.ResponseEntity;
import com.xbstar.types.Column;
import com.xbstar.types.Type;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class JDBCUtils
{
	public static void main(String[] args) throws SQLException
	{
		String jdbcAddress = "jdbc:sqlite:E:/火锅大数据核心治理框架/hotpot-potware/src/main/resources/warehouse.db";
		//		String jdbcAddress = "jdbc:mysql://192.168.1.152:3306/student";
		Connection connection = DriverManager.getConnection(jdbcAddress, "root", "123456");
		System.out.println(connection.getCatalog());
	}

	public static Connection makeMySQLConnection(String hostName, int hostPort, String username, String password, String database)
	{
		try
		{
			String url = "jdbc:mysql://" + hostName + ":" + hostPort;
			url = database == null ? url : url + "/" + database;
			Class.forName("com.mysql.cj.jdbc.Driver");
			DriverManager.setLoginTimeout(3);
			return DriverManager.getConnection(url, username, password);
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public static boolean validMySQLConfig(String hostName, int hostPort, String username, String password, String database)
	{
		try
		{
			Connection connection = makeMySQLConnection(hostName, hostPort, username, password, database);
			connection.close();
			return true;
		} catch (Exception var7)
		{
			return false;
		}
	}

	public static ResponseEntity checkMySQLConfig(String hostName, int hostPort, String username, String password, String database)
	{
		try
		{
			Connection connection = JDBCUtils.makeMySQLConnection(hostName, hostPort, username, password, database);
			ResultSet resultSet = connection.createStatement().executeQuery("SELECT VERSION()");
			resultSet.next();
			String version = resultSet.getString(1);
			version = version.contains("-") ? version.substring(0, version.indexOf("-")) : version;
			resultSet.close();
			connection.close();
			return new ResponseEntity(version, null, HttpStatus.OK);
		} catch (Exception e)
		{
			String message = e.getCause() == null ? e.getMessage() : e.getCause().toString().replace("\n", "");
			return new ResponseEntity(message, null, HttpStatus.ACCEPTED);
		}
	}

	public static String getDBMSType(Connection connection)
	{
		try
		{
			return connection.getMetaData().getDatabaseProductName().toLowerCase();
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public static String getJDBCAddress(Connection connection)
	{
		try
		{
			return connection.getMetaData().getURL();
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public static Set<String> getTableSet(Connection connection)
	{
		Set<String> result = new LinkedHashSet<>();
		try (Statement statement = connection.createStatement())
		{
			switch (getDBMSType(connection))
			{
				case "sqlite":
				{
					ResultSet cursor = statement.executeQuery("SELECT name FROM sqlite_master WHERE type='table'");
					while (cursor.next()) result.add(cursor.getString("name"));
					result.remove("sqlite_sequence"); //这张系统表不自动生成
					cursor.close();
					break;
				}
				case "mysql":
				{
					String database = connection.getCatalog();
					if (database == null || "".equals(database.trim())) throw new RuntimeException("[JDBC]没有传入库");
					ResultSet cursor = statement.executeQuery("SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '" + database + "'");
					while (cursor.next()) result.add(cursor.getString("TABLE_NAME"));
					cursor.close();
					break;
				}
				default:
				{
					throw new RuntimeException("[JDBC]其他数据类型" + getDBMSType(connection) + "暂时不支持");
				}
			}

		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
		return result;
	}

	public static Boolean getTableExist(Connection connection, String table)
	{
		boolean result = false;
		if (table == null || "".equals(table)) throw new RuntimeException("[JDBC]没有传入表");
		try (Statement statement = connection.createStatement())
		{
			switch (getDBMSType(connection))
			{
				case "sqlite":
				{
					ResultSet cursor = statement.executeQuery("SELECT name FROM sqlite_master WHERE type='table' AND name='" + table + "'");
					result = cursor.next();
					cursor.close();
					break;
				}
				case "mysql":
				{
					String database = connection.getCatalog();
					if (database == null || "".equals(database.trim())) throw new RuntimeException("[JDBC]没有传入库");
					String sql = "SELECT TABLE_NAME FROM information_schema.tables " + //
							"WHERE TABLE_TYPE = 'BASE TABLE' " + //
							"AND TABLE_SCHEMA = '" + database + "' " +//
							"AND TABLE_NAME = '" + table + "'";
					ResultSet cursor = connection.createStatement().executeQuery(sql);
					result = cursor.next();
					cursor.close();
					break;
				}
				default:
				{
					throw new RuntimeException("[JDBC]其他数据类型" + getDBMSType(connection) + "暂时不支持");
				}
			}
			return result;
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public static List<Column> getTableColumns(Connection connection, String table)
	{
		List<Column> result = new ArrayList<>();
		if (table == null || "".equals(table)) throw new RuntimeException("没有传入表");
		try (Statement statement = connection.createStatement())
		{
			switch (getDBMSType(connection))
			{
				case "sqlite":
				{
					ResultSet cursor = statement.executeQuery("PRAGMA table_info(" + table + ")");
					while (cursor.next())
					{
						Column column = new Column();
						column.type = Type.fromSQLiteType(cursor.getString("type"));
						column.table = table;
						column.field = cursor.getString("name");
						column.notnull = cursor.getBoolean("notnull");
						column.primary = cursor.getBoolean("pk");
						if (column.primary) column.columnIndex = "PRI:PRIMARY";
						column.visual = false;
						column.comment = ""; //SQLite取不到字段注释
						column.defaultValue = cursor.getString("dflt_value");
						result.add(column);
					}
					cursor.close();
					break;
				}
				case "mysql":
				{
					String database = connection.getCatalog();
					if (database == null || "".equals(database.trim())) throw new RuntimeException("没有传入库");
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
					while (cursor.next())
					{
						Column column = new Column();
						column.table = table;
						column.field = cursor.getString("COLUMN_NAME");
						column.notnull = "NO".equals(cursor.getString("IS_NULLABLE"));
						String type = cursor.getString("DATA_TYPE");
						Integer length = cursor.getInt("CHARACTER_MAXIMUM_LENGTH");
						Integer precision = cursor.getInt("NUMERIC_PRECISION");
						Integer scale = cursor.getInt("NUMERIC_SCALE");
						column.type = Type.fromMySQLType(type, precision == null ? length : precision, scale);
						//						column.put("sort", cursor.getInt("ORDINAL_POSITION"));
						column.visual = "VIRTUAL GENERATED".equals(cursor.getString("EXTRA")) || "STORED GENERATED".equals(cursor.getString("EXTRA"));
						column.comment = cursor.getString("COLUMN_COMMENT");
						String key = cursor.getString("COLUMN_KEY");//有PRI,MUL,UNI三种可能
						if (!"".equals(key))
						{
							ResultSet keyCur = connection.createStatement().executeQuery("SHOW INDEX FROM `" + table + "` WHERE Column_name = '" + column.field + "'");
							if (!keyCur.next()) throw new RuntimeException(table + "表中没有找到字段" + column + "的索引");
							String keyName = keyCur.getString("Key_name");
							keyCur.close();
							column.columnIndex = key + ":" + keyName;
						}
						result.add(column);
					}
					cursor.close();
				}
				default:
				{
					throw new RuntimeException("[JDBC]其他数据类型" + getDBMSType(connection) + "暂时不支持");
				}
			}

		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
		return result;
	}

	public static Set<String> getTablePrimaryKey(Connection connection, String table)
	{
		Set<String> result = new LinkedHashSet<>();
		if (table == null || "".equals(table)) throw new RuntimeException("没有传入表");
		try (Statement statement = connection.createStatement())
		{
			switch (getDBMSType(connection))
			{
				case "sqlite":
				{
					result.addAll(getTableColumns(connection, table).stream().filter(cur -> cur.primary).map(cur -> cur.field).collect(Collectors.toSet()));
					break;
				}
				case "mysql":
				{
					String database = connection.getCatalog();
					if (database == null || "".equals(database.trim())) throw new RuntimeException("没有传入库");
					String sql = "SELECT " //
							+ "`COLUMNS`.COLUMN_NAME, " //
							+ "`COLUMNS`.COLUMN_COMMENT "//
							+ "FROM " //
							+ "information_schema.`COLUMNS` " //
							+ "WHERE " //
							+ "`COLUMNS`.TABLE_SCHEMA = '" + database + "' AND" //
							+ "`COLUMNS`.COLUMN_KEY = 'PRI' AND" //
							+ "`COLUMNS`.TABLE_NAME = '" + table + "'";
					ResultSet cursor = statement.executeQuery(sql);
					while (cursor.next()) result.add(cursor.getString("COLUMN_NAME"));
					cursor.close();
					break;
				}
				default:
				{
					throw new RuntimeException("[JDBC]其他数据类型" + getDBMSType(connection) + "暂时不支持");
				}
			}
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
		return result;
	}
}
