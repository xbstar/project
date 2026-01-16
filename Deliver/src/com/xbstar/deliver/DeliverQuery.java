package com.xbstar.deliver;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

public class DeliverQuery<T extends Enum<?>>
{
	final Class<T> ADBTType;
	final List<Object> Where = new ArrayList<>();
	final Set<String> GroupBy = new LinkedHashSet<>();
	final String[] OrderBy = new String[] { null, null };

	private enum Logic
	{
		AND,
		OR
	}

	private enum Bracket
	{
		Left,
		Right;

		@Override
		public String toString()
		{
			if (this == Left) return "(";
			else return ")";
		}
	}

	private class Expression
	{
		T field;
		String append;

		public Expression(T field, String append)
		{
			this.field = field;
			this.append = append;
		}
	}

	private DeliverQuery(Class<T> adbt) {this.ADBTType = adbt;} //不允许外部调用只允许同包调用

	private String select() //构建SQL语句的SELECT子句
	{
		// 开始构建SQL语句
		Map<T, String> map = ADBTEngine.getADBTFieldMap(ADBTType);
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("SELECT ");
		Iterator<T> fieldIterator = map.keySet().iterator();
		while (fieldIterator.hasNext())
		{
			T field = fieldIterator.next();
			String from = map.get(field);
			if (from.contains(".")) sqlBuilder.append("`" + from.split("\\.")[0] + "`.`" + from.split("\\.")[1] + "`");
			else sqlBuilder.append("`" + from + "`");
			if (!field.equals(from)) sqlBuilder.append(" AS `" + field + "`");
			if (fieldIterator.hasNext()) sqlBuilder.append(",");
		}
		return sqlBuilder.toString();
	}

	private String from() //构造SQL语句的FROM和JOIN子句
	{
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append(" FROM ");
		if (ADBTEngine.isRealTable(ADBTType)) sqlBuilder.append("`" + ADBTEngine.getRealTableName(ADBTType) + "`");
		else
		{
			sqlBuilder.append("`" + ADBTEngine.getViewBaseTable(ADBTType) + "`");
			Map<String, List<String>> left = ADBTEngine.getViewLeftJoin(ADBTType);
			for (String joinTable : left.keySet())
			{
				sqlBuilder.append(" LEFT JOIN `" + joinTable + "`");
				Iterator<String> onIterator = left.get(joinTable).iterator();
				if (onIterator.hasNext()) sqlBuilder.append(" ON ");
				while (onIterator.hasNext())
				{
					sqlBuilder.append(onIterator.next());
					if (onIterator.hasNext()) sqlBuilder.append(" AND ");
				}
			}
		}
		return sqlBuilder.toString();
	}

	private String where() //构造SQL语句的Where子句
	{
		if (Where.size() <= 0) return "";
		// 处理左右括号不匹配问题
		long leftBracket = Where.stream().filter(cur -> cur == Bracket.Left).count();
		long rightBracket = Where.stream().filter(cur -> cur == Bracket.Right).count();
		if (rightBracket > leftBracket) for (long i = 0; i < rightBracket - leftBracket; i++) Where.add(0, Bracket.Left);
		if (rightBracket < leftBracket) for (long i = 0; i < leftBracket - rightBracket; i++) Where.add(Bracket.Right);
		// 构建WHERE子句
		Map<T, String> map = ADBTEngine.getADBTFieldMap(ADBTType);
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append(" WHERE ");
		for (Object obj : Where)
		{
			if (obj instanceof DeliverQuery.Expression)
			{
				Expression exp = (Expression) obj;
				String field = map.get(exp.field);
				if (field.contains(".")) sqlBuilder.append("`" + field.split("\\.")[0] + "`.`" + field.split("\\.")[1] + "`" + exp.append + " ");
				else sqlBuilder.append("`" + field + "`" + exp.append + " ");
			}
			else sqlBuilder.append(obj + " ");
		}
		return sqlBuilder.toString();
	}

	private String group(Set<String> groupBy) //生成SQL的GroupBy子句
	{
		if (groupBy.size() <= 0) return "";
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append(" GROUP BY ");
		Iterator<String> groupIterator = groupBy.iterator();
		while (groupIterator.hasNext())
		{
			sqlBuilder.append(groupIterator.next());
			if (groupIterator.hasNext()) sqlBuilder.append(",");
		}
		sqlBuilder.append(" ");
		return sqlBuilder.toString();
	}

	private String order(String field, String asc_desc) //生成SQL的ORDER BY子句
	{
		if (field == null || asc_desc == null) return "";
		return " ORDER BY `" + field + "` " + asc_desc + " ";
	}

	private String limit(Integer index, Integer volume) //生成SQL的Limit子句
	{
		if (index != null && volume != null) //指定了页码和每页数量需要计算便宜
		{
			if (index < 1) index = 1; //保障页不越下界，上界如果越界就查不出任何数据
			int offset = (index - 1) * volume;
			return "LIMIT " + offset + "," + volume;
		}
		else if (volume != null) return "LIMIT " + volume; //指定了每页数量，没指定第几页，返回第1页的数据
		else if (index != null) return ""; //没有限定每页数量，但指定要第几页还是返回所有数据
		else return ""; //啥都没有返回所有数据
	}

	public int delete()
	{
		if (!ADBTEngine.isRealTable(ADBTType)) throw new RuntimeException("[Deliver]视图" + this + "不支持执行批量删除");
		if (Where.stream().filter(cur -> cur instanceof DeliverQuery.Expression).count() <= 0) throw new RuntimeException("[Deliver]没有Where条件会把整张表的数据删除");
		String deleteSQL = "DELETE FROM `" + ADBTEngine.getRealTableName(ADBTType) + "`" + where();
		System.out.println(deleteSQL);
		try (Statement statement = ADBTEngine.getJDBCConnection().createStatement())
		{
			return statement.executeUpdate(deleteSQL);
		} catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	public List<Deliver<T>> query()
	{
		return this.queryPage(null, null);
	}

	public Deliver<T> queryDeliver(String id)
	{
		if (!ADBTEngine.isRealTable(ADBTType)) throw new RuntimeException("[Deliver]视图数据无法通过主键查找获取");
		Set<T> pkSet = ADBTEngine.getADBTPrimarySet(ADBTType);
		if (pkSet.size() > 1) throw new RuntimeException("[Deliver]" + ADBTType.getSimpleName() + "关联的" + ADBTEngine.getRealTableName(ADBTType) + "是多主键表,需要分别传入所有的主键值");
		else
		{
			T primaryKey = pkSet.iterator().next();
			Deliver<T> deliver = new Deliver<>(ADBTType);
			deliver.PrimaryStore.put(primaryKey, id);
			return deliver;
		}
	}

	public Deliver<T> queryDeliver(Map<T, String> ids)
	{
		if (!ADBTEngine.isRealTable(ADBTType)) throw new RuntimeException("[Deliver]视图数据无法通过主键查找获取");
		Set<String> passSet = ids.keySet().stream().map(cur -> cur.name()).collect(Collectors.toSet());
		Set<String> pkSet = ADBTEngine.getADBTPrimarySet(ADBTType).stream().map(cur -> cur.name()).collect(Collectors.toSet());
		if (passSet.containsAll(pkSet))
		{
			Deliver<T> deliver = new Deliver<>(ADBTType);
			for (T pk : ids.keySet()) deliver.PrimaryStore.put(pk, ids.get(pk));
			return deliver;
		}
		else
		{
			pkSet.removeAll(passSet);
			throw new RuntimeException("[Deliver]缺失了主键" + pkSet.iterator().next() + "的值填充");
		}
	}

	public List<String> queryField(T field)
	{
		String querySQL = "SELECT `" + field.name() + "`" + from() + where() + group(GroupBy) + order(OrderBy[0], OrderBy[1]);
		System.out.println(querySQL);
		List<String> result = new ArrayList<>();
		try (Statement statement = ADBTEngine.getJDBCConnection().createStatement())
		{
			ResultSet cursor = statement.executeQuery(querySQL);
			while (cursor.next()) result.add(cursor.getString(1));
			cursor.close();
		} catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
		return result;
	}

	public List<Deliver<T>> queryLimit(int limit)
	{
		return this.queryPage(null, limit);
	}

	public List<Deliver<T>> queryPage(Integer index, Integer volume)
	{
		// 开始查询和填充数据
		String querySQL = select() + from() + where() + group(this.GroupBy) + order(OrderBy[0], OrderBy[1]) + limit(index, volume);
		System.out.println(querySQL);
		List<Deliver<T>> result = new LinkedList<>();
		Map<T, String> map = ADBTEngine.getADBTFieldMap(ADBTType);
		try (Statement statement = ADBTEngine.getJDBCConnection().createStatement())
		{
			ResultSet cursor = statement.executeQuery(querySQL);
			while (cursor.next())
			{
				Deliver<T> deliver = new Deliver<>(ADBTType);
				for (T pk : ADBTEngine.getADBTPrimarySet(ADBTType)) deliver.PrimaryStore.put(pk, cursor.getString(pk.name()));
				for (T fi : map.keySet()) deliver.DeliverStore.put(fi, cursor.getString(fi.name()));
				result.add(deliver);
			}
			cursor.close();
		} catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
		return result;
	}

	public DeliverPage<T> queryDeliverPage(int index, int volume)
	{
		DeliverPage<T> result = new DeliverPage<T>();
		String countSQL = "SELECT COUNT(*) FROM (" + select() + from() + where() + group(this.GroupBy) + ")";//先不要Limit统计出总数量,Orderby对计算没有影响页不用加
		// 首先计算数量
		try (Statement statement = ADBTEngine.getJDBCConnection().createStatement())
		{
			ResultSet cursor = statement.executeQuery(countSQL);
			if (cursor.next()) result.count = cursor.getLong(1);
			cursor.close();
		} catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
		// 然后获得数据
		result.data = this.queryPage(index, volume);
		return result;
	}

	public DeliverQuery<T> eq(T field, Object value)
	{
		Object last = Where.size() > 0 ? Where.get(Where.size() - 1) : Bracket.Left;
		if (last instanceof DeliverQuery.Expression || last == Bracket.Right) Where.add(Logic.AND);
		String append = "=";
		if (value == null) append = "is null";
		else if (value instanceof Number) append += value;
		else append += "'" + value + "'";
		Where.add(new Expression(field, append));
		return this;
	}

	public DeliverQuery<T> ne(T field, Object value)
	{
		Object last = !Where.isEmpty() ? Where.get(Where.size() - 1) : Bracket.Left;
		if (last instanceof DeliverQuery.Expression || last == Bracket.Right) Where.add(Logic.AND);
		String append = "!=";
		if (value == null) append = "is not null";
		else if (value instanceof Number) append += value;
		else append += "'" + value + "'";
		Where.add(new Expression(field, append));
		return this;
	}

	public DeliverQuery<T> like(T field, Object value)
	{
		if (value == null) return this;
		Object last = !Where.isEmpty() ? Where.get(Where.size() - 1) : Bracket.Left;
		if (last instanceof DeliverQuery.Expression || last == Bracket.Right) Where.add(Logic.AND);
		String append = "like '%" + value + "%'";
		Where.add(new Expression(field, append));
		return this;
	}

	public DeliverQuery<T> likeStart(T field, Object value)
	{
		if (value == null) return this;
		Object last = !Where.isEmpty() ? Where.get(Where.size() - 1) : Bracket.Left;
		if (last instanceof DeliverQuery.Expression || last == Bracket.Right) Where.add(Logic.AND);
		String append = "like '" + value + "%'";
		Where.add(new Expression(field, append));
		return this;
	}

	public DeliverQuery<T> likeEnd(T field, Object value)
	{
		if (value == null) return this;
		Object last = !Where.isEmpty() ? Where.get(Where.size() - 1) : Bracket.Left;
		if (last instanceof DeliverQuery.Expression || last == Bracket.Right) Where.add(Logic.AND);
		String append = "like '%" + value + "'";
		Where.add(new Expression(field, append));
		return this;
	}

	public DeliverQuery<T> leftBracket()
	{
		Object last = !Where.isEmpty() ? Where.get(Where.size() - 1) : Bracket.Left;
		if (last instanceof String || last == Bracket.Right) Where.add(Logic.AND);
		Where.add(Bracket.Left);
		return this;
	}

	public DeliverQuery<T> rightBracket()
	{
		Object last = !Where.isEmpty() ? Where.get(Where.size() - 1) : Bracket.Left;
		if (last == Bracket.Left) Where.remove(Where.size() - 1);
		else Where.add(Bracket.Right);
		return this;
	}

	public DeliverQuery<T> and()
	{
		Object last = !Where.isEmpty() ? Where.get(Where.size() - 1) : Bracket.Left;
		if (last instanceof Logic || last == Bracket.Left) return this;
		Where.add(Logic.AND);
		return this;
	}

	public DeliverQuery<T> or()
	{
		Object last = !Where.isEmpty() ? Where.get(Where.size() - 1) : Bracket.Left;
		if (last instanceof Logic || last == Bracket.Left) return this;
		Where.add(Logic.OR);
		return this;
	}

	public static <T extends Enum<?>> DeliverQuery<T> of(Class<T> cla)
	{
		return new DeliverQuery<>(cla);
	}
}
