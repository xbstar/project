package com.xbstar;

import org.apache.flink.shaded.hadoop2.com.google.gson.internal.LinkedTreeMap;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import java.util.*;
import java.util.stream.Collectors;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

public class FlinkTableSchema implements Serializable
{
	/* 最基本的数据库名和表名*/
	private String tableName;
	/* Flink原生的列数据类型和FlinkTable行类型，需要序列化*/
	private Map<String, DataType> columnsFieldMap; //如果用List来表述就是List<DataType.Field>，一个Field就包含了Key和DataType,但为了让不了解Flink的人也能一眼看懂，最终还是选择了Map数据结构
	private Set<String> primaryKeySet;
	private Set<String> notNullNameSet;
	/* 临时生成的转化器，不需要序列化*/
	private transient RowRowConverter converter;

	/**
	 * 使用ResolvedSchema构造
	 * @param schema 能够从catalogTable中获取
	 */
	public FlinkTableSchema(String tableName, ResolvedSchema schema)
	{
		this(tableName, (Map<String, DataType>) schema.getColumnNames().stream().collect(Collectors.toMap(key -> key, key -> schema.getColumn(key).get().getDataType(), (v1, v2) -> v1, LinkedHashMap::new)), null, null);
	}

	public FlinkTableSchema(String tableName, List<DataTypes.Field> fieldList, Set<String> primaryKey, Set<String> notNullSet)
	{
		this(tableName, (Map<String, DataType>) fieldList.stream().collect(Collectors.toMap(filed -> filed.getName(), field -> field.getDataType(), (v1, v2) -> v1, LinkedHashMap::new)), primaryKey, notNullSet);
	}

	/**
	 * 使用filed定义字段,这个函数主要用于手动创建Schema
	 * @param columns 所有列名称和类型
	 * @param primaryKey 表主键，可以有多个
	 */
	public FlinkTableSchema(String tableName, Map<String, DataType> columns, Set<String> primaryKey, Set<String> notNullSet)
	{
		// 上面所有构造函数最终统一到这个构造函数
		this.tableName = tableName;
		this.primaryKeySet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
		this.notNullNameSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
		this.columnsFieldMap = new LinkedTreeMap<>(String.CASE_INSENSITIVE_ORDER);
		this.primaryKeySet.addAll(primaryKey);
		this.notNullNameSet.addAll(notNullSet);
		this.columnsFieldMap.putAll(columns);
		// 进行检查判断
		if (this.tableName == null)
		{
			throw new IllegalArgumentException("必须配置表名");
		}
		if (this.columnsFieldMap.size() <= 0)
		{
			System.out.println("\033[1;31m" + this.tableName + " 表的配置至少需要包含一个列" + "\033[0m");
			throw new IllegalArgumentException(this.tableName + " 表的配置至少需要包含一个列");
		}
		Set<String> fieldNames = this.columnsFieldMap.keySet();
		for (String pk : this.primaryKeySet)
		{
			if (!fieldNames.contains(pk))
			{
				System.out.println("\033[1;31m" + "主键 " + pk + " 在" + this.tableName + "的字段声明 " + this
						.getRowColumnNames() + " 中不存在" + "\033[0m");
				throw new IllegalArgumentException("主键 " + pk + " 在" + this.tableName + "的字段声明 " + this.getRowColumnNames() + " 中不存在");
			}
		}
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		FlinkTableSchema schema = (FlinkTableSchema) o;
		return Objects.equals(tableName, schema.tableName) &&
				Objects.equals(columnsFieldMap, schema.columnsFieldMap) &&
				Objects.equals(primaryKeySet, schema.primaryKeySet) &&
				Objects.equals(notNullNameSet, schema.notNullNameSet);
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(tableName, columnsFieldMap, primaryKeySet, notNullNameSet);
	}

	/** 获取字段数量*/
	public int size()
	{
		return this.columnsFieldMap.size();
	}

	/**
	 * 获得DyanmicTable配置的所有的列名
	 * @return 列明按照定义顺序组成的列表
	 */
	public Set<String> getRowColumnNames()
	{
		return this.columnsFieldMap.keySet();
	}

	/** 获得所有主键字段的名称*/
	public Set<String> getPrimaryKeyNames()
	{
		return this.primaryKeySet;
	}

	/** 获得所有非空字段的名称*/
	public Set<String> getNotNullColumnNames()
	{
		return this.notNullNameSet;
	}

	/**
	 * 获取指名称的表字段
	 * @param columnName 指定列名
	 * @return Flink表字段
	 */
	public RowType.RowField getFieldOfColumn(String columnName)
	{
		RowType rowType = this.getRowType();
		return rowType.getFields().get(rowType.getFieldIndex(columnName));
	}

	/**
	 * 根据列名确定列索引位置
	 * @param columnName 列名
	 * @return 列所在的索引位置（就是第几列)
	 */
	public int getPositionOfColumn(String columnName)
	{
		return this.getRowType().getFieldNames().indexOf(columnName);
	}

	/**
	 * 获取指定位置的表字段
	 * @param position 位置
	 * @return Flink表字段
	 */
	public RowType.RowField getFieldAtPosition(int position)
	{
		return this.getRowType().getFields().get(position);
	}

	/**
	 * 获取指定位置字段的名称
	 * @param position 指定位置索引
	 * @return 字段名称
	 */
	public String getColumnNameAtPosition(int position)
	{
		return this.getFieldAtPosition(position).getName();
	}

	/**
	 * 获取指定列名字段的数据类型
	 * @param columnName 指定列名称
	 * @return 数据类型
	 */
	public DataType getDataTypeOfColumn(String columnName)
	{
		return this.columnsFieldMap.get(columnName);
	}

	/**
	 * 获取指定索引字段的数据类型
	 * @param position 索引位置
	 * @return 数据类型
	 */
	public DataType getDataTypeAtPosition(int position)
	{
		String columnName = this.columnsFieldMap.keySet().stream().collect(Collectors.toList()).get(position);
		return this.columnsFieldMap.get(columnName);
	}

	/**
	 * 获取表行的Flink标准数据类型
	 * @return Flink表行（DataTypes.Row)标准数据类型
	 */
	public DataType getRowDataType()
	{
		List<DataTypes.Field> fieldList = this.columnsFieldMap.keySet().stream().map(key -> DataTypes.FIELD(key, this.columnsFieldMap.get(key))).collect(Collectors.toList());
		return DataTypes.ROW(fieldList);
	}

	/**
	 * 获取行数据类型，专门为FlinkDynamicTable定义的类型
	 * @return 行数据类型
	 */
	public RowType getRowType()
	{
		return (RowType) this.getRowDataType().getLogicalType();
	}

	/**
	 * 获得列的数量
	 * @return 列个数
	 */
	public int getColumnSize()
	{
		return this.columnsFieldMap.size();
	}

	/** 获取表名字*/
	public String getTableName()
	{
		return this.tableName;
	}

	/**
	 * 根据当前的Schema转化Row为RowData
	 * @param row row的数据
	 * @return 转化后的RowData数据
	 */
	public RowData convertRowToRowData(Row row)
	{
		if (this.converter == null)
		{
			this.converter = RowRowConverter.create(this.getRowDataType());
		}
		return this.converter.toInternal(row);
	}

	/**
	 * 根据当前的Schema转化RowData为Row
	 * @param rowData rowData的数据
	 * @return 转化后的Row数据
	 */
	public Row convertRowDataToRow(RowData rowData)
	{
		if (this.converter == null)
		{
			this.converter = RowRowConverter.create(this.getRowDataType());
		}
		return this.converter.toExternal(rowData);
	}

	/** 根据FlinkTableSchema中的元数据信息，在指定Connection的数据库中创建一张表*/
	public void createTableInDataBase(Connection connection) throws SQLException
	{
		CabbageUtils.createTableToMySQL(connection, this);
	}
}
