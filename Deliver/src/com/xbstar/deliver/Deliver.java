package com.xbstar.deliver;

import com.alibaba.fastjson.JSONObject;
import com.xbstar.utils.PrintUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Deliver<T extends Enum<?>>
{
	public final Class<T> ADBTType;
	final Map<T, String> PrimaryStore = new LinkedHashMap<>();
	final Map<T, String> DeliverStore = new LinkedHashMap<>();
	final Map<String, Object> AttachStore = new LinkedHashMap<>();

	Deliver(Class<T> adbt) {this.ADBTType = adbt;} //不允许外部调用只允许同包构建

	public Deliver(Class<T> adbt, String id)
	{
		this.ADBTType = adbt;
		if (!ADBTEngine.isRealTable(ADBTType)) throw new RuntimeException("[Deliver]视图数据不能直接初始化");
		Set<T> pkSet = ADBTEngine.getADBTPrimarySet(ADBTType);
		if (pkSet.size() > 1) throw new RuntimeException("[Deliver]" + ADBTType.getSimpleName() + "关联的" + ADBTEngine.getRealTableName(ADBTType) + "是多主键表,需要分别传入所有的主键值");
		T primaryKey = pkSet.iterator().next();
		this.PrimaryStore.put(primaryKey, id);
		this.DeliverStore.put(primaryKey, id);
	}

	public Deliver(Class<T> adbt, Map<T, String> ids)
	{
		this.ADBTType = adbt;
		if (!ADBTEngine.isRealTable(ADBTType)) throw new RuntimeException("[Deliver]视图数据不能直接初始化");
		Deliver<T> deliver = new Deliver<>(ADBTType);
		for (T pk : ids.keySet())
		{
			deliver.PrimaryStore.put(pk, ids.get(pk));
			deliver.DeliverStore.put(pk, ids.get(pk));
		}
	}

	public void print()
	{
		PrintUtils.printVerbose("--------------" + this.toString() + "-----------------");
		for (T key : this.DeliverStore.keySet())
		{
			StringBuilder hint = new StringBuilder();
			int tabNumber = (16 - key.name().length()) / 4;
			if (key.name().length() % 4 == 0) tabNumber--;
			hint.append(key);
			for (int i = 0; i < tabNumber; i++) hint.append("\t");
			hint.append(DeliverStore.get(key));
			PrintUtils.printVerbose(hint.toString());
		}
		Set<String> attachKey = this.AttachStore.keySet();
		attachKey.removeAll(DeliverStore.keySet().stream().map(cur -> cur.name()).collect(Collectors.toSet()));
		if (attachKey.size() <= 0) return;
		PrintUtils.printVerbose("Attach Data:-----------------");
		for (String key : attachKey)
		{
			StringBuilder hint = new StringBuilder();
			int tabNumber = (16 - key.length()) / 4;
			if (key.length() % 4 == 0) tabNumber--;
			hint.append(key);
			for (int i = 0; i < tabNumber; i++) hint.append("\t");
			hint.append(AttachStore.get(key));
			PrintUtils.printVerbose(hint.toString());
		}
	}

	public boolean exist()
	{
		if (!ADBTEngine.isRealTable(ADBTType)) return false; //视图数据是虚拟的不存在
		if (!this.complete()) return false; // 主键不完备则不存在
		StringBuilder sqlBuilder = new StringBuilder();
		Set<T> primarySet = ADBTEngine.getADBTPrimarySet(ADBTType);
		String tableRealName = ADBTEngine.getRealTableName(ADBTType);
		sqlBuilder.append("SELECT COUNT(*) FROM `").append(tableRealName);
		StringBuilder whereBuilder = new StringBuilder();
		whereBuilder.append("` WHERE ");
		Iterator<T> primaryIterator = primarySet.iterator();
		while (primaryIterator.hasNext())
		{
			T pk = primaryIterator.next();
			whereBuilder.append("`").append(pk).append("`='").append(PrimaryStore.get(pk)).append("'");
			if (primaryIterator.hasNext()) whereBuilder.append(" AND ");
		}
		try (Statement statement = ADBTEngine.getJDBCConnection().createStatement())
		{
			ResultSet cursor = statement.executeQuery("" + sqlBuilder + whereBuilder);
			cursor.next();
			boolean result = cursor.getInt(1) >= 1;
			cursor.close();
			if (result) System.out.println("EXISTS " + tableRealName + whereBuilder);
			else System.out.println("NOTONE " + tableRealName + whereBuilder);
			return result;
		} catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	public Deliver<T> inflate()
	{
		if (!this.complete()) return this;
		// 判断是否有未填充字段
		Map<T, String> map = ADBTEngine.getADBTFieldMap(ADBTType);
		if (DeliverStore.keySet().containsAll(map.keySet())) return this;
		// 发现存在未填充的字段发起一次查询
		DeliverQuery<T> query = DeliverQuery.of(ADBTType);
		for (T key : PrimaryStore.keySet()) query.eq(key, PrimaryStore.get(key));
		List<Deliver<T>> resultList = query.query();
		if (!resultList.isEmpty())
		{
			Map<T, String> inf = resultList.get(0).DeliverStore;
			for (T key : inf.keySet())
			{
				if (this.DeliverStore.containsKey(key)) continue;
				this.DeliverStore.put(key, inf.get(key));
			}
		}
		else PrintUtils.printWarning("[Deliver]" + ADBTType.getSimpleName() + "关联的" + ADBTEngine.getRealTableName(ADBTType) + "表中不存在主键为" + this.PrimaryStore + "的记录");
		return this;
	}

	public Boolean complete() //判断主键是否完备
	{
		if (ADBTEngine.isRealTable(ADBTType))
		{
			Set<T> declarePKSet = ADBTEngine.getADBTPrimarySet(ADBTType);
			if (this.PrimaryStore.keySet().containsAll(declarePKSet)) return true;
			else
			{
				for (T pk : declarePKSet)
				{
					if (PrimaryStore.containsKey(pk)) continue;
					PrintUtils.printWarning("[Deliver]" + this + "缺失了主键值" + pk + "无法执行填充和更新");
					break;
				}
				return false;
			}
		}
		else
		{
			PrintUtils.printWarning("[Deliver]" + this + "是视图因此无法执行填充和更新");
			return false;
		}
	}

	public Deliver<T> cpk(T field, Object value)
	{
		this.DeliverStore.put(field, value == null ? null : String.valueOf(value));
		// 如果传入的是主键也要一并修改
		if (value == null) return this;
		Set<T> primaryKeySet = ADBTEngine.getADBTPrimarySet(ADBTType);
		if (primaryKeySet.contains(field)) PrimaryStore.put(field, String.valueOf(value));
		return this;
	}

	public Deliver<T> put(T field, Object value)
	{
		this.DeliverStore.put(field, value == null ? null : String.valueOf(value));
		// 如果传入的是主键且主键尚未填充则填充
		if (value == null) return this;
		Set<T> primaryKeySet = ADBTEngine.getADBTPrimarySet(ADBTType);
		if (primaryKeySet.contains(field) && !PrimaryStore.containsKey(field)) PrimaryStore.put(field, String.valueOf(value));
		return this;
	}

	public void attach(String filed, Object value)
	{
		this.AttachStore.put(filed, value);
	}

	public Object getAttach(String field)
	{
		return this.AttachStore.get(field);
	}

	public <M extends Enum<?>> Deliver<M> getDeliver(Class<M> cla, T field)
	{
		if (!ADBTEngine.isRealTable(cla)) throw new RuntimeException("[Deliver]" + cla.getName() + "不是数据表");
		return new Deliver<>(cla, this.getString(field));
	}

	public String getString(T field)
	{
		if (!this.DeliverStore.containsKey(field)) this.inflate();
		return this.DeliverStore.get(field);
	}

	public Integer getInteger(T field)
	{
		if (!this.DeliverStore.containsKey(field)) this.inflate();
		String value = DeliverStore.get(field);
		if (value == null) return null;
		Pattern pattern = Pattern.compile("\\d+");
		Matcher matcher = pattern.matcher(value);
		String longestNumber = "";
		while (matcher.find())
		{
			String current = matcher.group();
			if (current.length() > longestNumber.length())
			{
				longestNumber = current;
			}
		}
		if (longestNumber.isEmpty()) return null;
		return Integer.valueOf(longestNumber);
	}

	public int getInt(T field)
	{
		Integer result = getInteger(field);
		return result == null ? 0 : result;
	}

	public Boolean upsert()
	{
		// 开始构建SQL语句
		Map<T, String> map = ADBTEngine.getADBTFieldMap(ADBTType);
		StringBuilder sqlBuilder = new StringBuilder();
		if (exist())
		{
			// 继续构建更新语句
			sqlBuilder.append("UPDATE `").append(ADBTEngine.getRealTableName(ADBTType)).append("` SET ");
			Iterator<T> fieldIterator = DeliverStore.keySet().iterator();
			while (fieldIterator.hasNext())
			{
				T field = fieldIterator.next();
				String value = DeliverStore.get(field);
				if (value != null) value = "'" + value + "'";
				sqlBuilder.append("`").append(map.get(field)).append("`=").append(value);
				if (fieldIterator.hasNext()) sqlBuilder.append(",");
			}
			sqlBuilder.append(" WHERE ");
			Set<T> primaryKeySet = ADBTEngine.getADBTPrimarySet(ADBTType);
			Iterator<T> primaryIterator = primaryKeySet.iterator();
			while (primaryIterator.hasNext())
			{
				T pk = primaryIterator.next();
				sqlBuilder.append("`" + pk.name() + "`='" + PrimaryStore.get(pk) + "'");
				if (primaryIterator.hasNext()) sqlBuilder.append(" AND ");
			}
		}
		else
		{
			sqlBuilder.append("INSERT INTO `").append(ADBTEngine.getRealTableName(ADBTType)).append("` (");
			Iterator<String> columnIterator = map.values().iterator();
			while (columnIterator.hasNext())
			{
				sqlBuilder.append("`").append(columnIterator.next()).append("`");
				if (columnIterator.hasNext()) sqlBuilder.append(",");
			}
			sqlBuilder.append(") VALUES (");
			Iterator<T> fieldIterator = map.keySet().iterator();
			while (fieldIterator.hasNext())
			{
				String value = DeliverStore.get(fieldIterator.next());
				if (value == null) value = "null";
				else value = "'" + value + "'";
				sqlBuilder.append(value);
				if (fieldIterator.hasNext()) sqlBuilder.append(",");
			}
			sqlBuilder.append(")");
		}
		System.out.println(sqlBuilder.toString());
		try (Statement statement = ADBTEngine.getJDBCConnection().createStatement())
		{
			boolean result = statement.execute(sqlBuilder.toString());
			if (result) for (T filed : PrimaryStore.keySet()) PrimaryStore.put(filed, DeliverStore.get(filed)); //可能存在主键变更的情况需要更新同步
			return result;
		} catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	public Boolean delete()
	{
		if (!this.complete()) return false;
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("DELETE FROM `").append(ADBTEngine.getRealTableName(ADBTType)).append("`");
		Map<T, String> map = ADBTEngine.getADBTFieldMap(ADBTType);
		sqlBuilder.append(" WHERE ");
		Iterator<T> primaryIterator = ADBTEngine.getADBTPrimarySet(ADBTType).iterator();
		while (primaryIterator.hasNext())
		{
			T pk = primaryIterator.next();
			sqlBuilder.append("`").append(map.get(pk)).append("`='").append(PrimaryStore.get(pk)).append("'");
			if (primaryIterator.hasNext()) sqlBuilder.append(" AND ");
		}
		System.out.println(sqlBuilder.toString());
		try (Statement statement = ADBTEngine.getJDBCConnection().createStatement())
		{
			return statement.execute(sqlBuilder.toString());
		} catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public String toString()
	{
		return "Deliver<" + ADBTType.getSimpleName() + ">";
	}

	public JSONObject toJSON()
	{
		JSONObject result = new JSONObject();
		result.putAll(toMap());
		return result;
	}

	public Map<String, Object> toMap()
	{
		if (ADBTEngine.isRealTable(ADBTType)) this.inflate();
		Map<String, Object> result = new LinkedHashMap<>();
		result.putAll(this.AttachStore); //Attach优先级最低如果存在同名键会被覆盖
		for (T key : DeliverStore.keySet()) result.put(key.name(), DeliverStore.get(key)); //将Deliver的数据导出
		result.put("adbt", ADBTType.getName()); //ADBT类型是一定一定要有的
		return result;
	}

	public static <T extends Enum<?>> Deliver<T> fromMap(Map<String, Object> map)
	{
		Class<T> adbt;
		try {adbt = (Class<T>) Class.forName(map.get("adbt").toString());} catch (ClassNotFoundException e) {throw new RuntimeException(e);}
		return fromMap(map, adbt);
	}

	public static <T extends Enum<?>> Deliver<T> fromMap(Map<String, Object> map, Class<T> cla)
	{
		Deliver<T> result = new Deliver<>(cla);
		for (T field : ADBTEngine.getADBTFieldMap(cla).keySet())
		{
			Object value = map.get(field.name());
			if (value == null) continue;
			result.put(field, value);
			map.remove(field.name());
		}
		result.AttachStore.putAll(map);
		return result;
	}

	public static <T extends Enum<?>> Deliver<T> fromJSON(JSONObject json, Class<T> cla)
	{
		Deliver<T> result = new Deliver<>(cla);
		for (T field : ADBTEngine.getADBTFieldMap(cla).keySet())
		{
			Object value = json.get(field.name());
			if (value == null) continue;
			result.put(field, value);
			json.remove(field.name());
		}
		for (String key : json.keySet()) result.AttachStore.put(key, json.get(key));
		return result;
	}
}
