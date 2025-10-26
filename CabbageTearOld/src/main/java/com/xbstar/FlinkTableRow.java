package com.xbstar;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.Serializable;
import java.util.*;

/**
 * 涵盖了表信息的记录数据结构
 * Flink原生的Row和RowData都不能记录表名，这样就无法处理一个算子需要多表处理的情况（比如一个source算子要捕获多张表的数据）
 * 为了在捕获的时候记录这是哪一张表的数据，我们自定义一个数据结构
 */
public class FlinkTableRow implements Serializable
{
	private FlinkTableSchema tableSchema;
	private RowKind rowKind;
	private Object[] rowValuse;

	public FlinkTableRow(FlinkTableSchema schema, RowKind rowKind)
	{
		this.tableSchema = schema;
		this.rowKind = rowKind;
		this.rowValuse = new Object[schema.getColumnSize()];
	}

	/** 获取键值对数量*/
	public int size()
	{
		return this.tableSchema.size();
	}

	/** 获取表名字*/
	public String getTableName()
	{
		return this.tableSchema.getTableName();
	}

	public FlinkTableSchema getSchema()
	{
		return this.tableSchema;
	}

	public RowKind getRowKind()
	{
		return this.rowKind;
	}

	public Object getRowValueAtPosition(int position)
	{
		return this.rowValuse[position];
	}

	public Object getRowValueOfColumn(String columnName)
	{
		return this.rowValuse[this.tableSchema.getPositionOfColumn(columnName)];
	}

	public void setRowValueAtPosition(int position, Object value)
	{
		this.rowValuse[position] = value;
	}

	public void setRowValueOfColumn(String columnName, Object value)
	{
		this.rowValuse[this.tableSchema.getPositionOfColumn(columnName)] = value;
	}

	public RowData convertToRowData()
	{
		return this.tableSchema.convertRowToRowData(this.convertToRow());
	}

	public Row convertToRow()
	{
		Row row = Row.withNames(this.rowKind);
		for (String column : this.tableSchema.getRowColumnNames())
		{
			row.setField(column, this.getRowValueOfColumn(column));
		}
		return row;
	}

	@Override
	public int hashCode()
	{
		return Arrays.hashCode(rowValuse);
	}

	@Override
	public String toString()
	{
		StringBuilder build = new StringBuilder();
		build.append(this.tableSchema.getTableName());
		build.append(" ");
		if (this.rowKind == RowKind.INSERT) build.append("+I");
		if (this.rowKind == RowKind.UPDATE_BEFORE) build.append("-U");
		if (this.rowKind == RowKind.UPDATE_AFTER) build.append("+U");
		if (this.rowKind == RowKind.DELETE) build.append("-D");
		build.append("{");
		Iterator<String> it = this.getSchema().getRowColumnNames().iterator();
		while (it.hasNext())
		{
			String column = it.next();
			DataType type = this.tableSchema.getDataTypeOfColumn(column);
			Object value = this.getRowValueOfColumn(column);
			build.append(type + ":" + column + "=" + value);
			if (it.hasNext())
			{
				build.append(", ");
			}
		}
		build.append("}");
		return build.toString();
	}

	/** 从RowData转化为FlinkTableRow*/
	public static FlinkTableRow createFormRowData(FlinkTableSchema schema, RowData rowdata)
	{
		Row row = schema.convertRowDataToRow(rowdata);
		return FlinkTableRow.createFromRow(schema, row);
	}

	/** 从Row转化为FlinkTableRow，Schema中所有Field的数据类型都为string*/
	public static FlinkTableRow createFromRow(String tableName, Row row)
	{
		List<DataTypes.Field> fieldList = new ArrayList<>();
		for (String column : row.getFieldNames(true))
		{
			fieldList.add(DataTypes.FIELD(column, DataTypes.STRING()));
		}
		FlinkTableSchema schema = new FlinkTableSchema(tableName, fieldList, new TreeSet<>(), new TreeSet<>());
		return FlinkTableRow.createFromRow(schema, row);
	}

	/** 从Row转化为FlinkTableRow*/
	public static FlinkTableRow createFromRow(FlinkTableSchema schema, Row row)
	{
		FlinkTableRow result = new FlinkTableRow(schema, row.getKind());
		for (String column : row.getFieldNames(true))
		{
			if (!schema.getRowColumnNames().contains(column))
			{
				throw new IllegalArgumentException("row数据数包含了不在Schema定义中的列" + column);
			}
			result.setRowValueOfColumn(column, row.getField(column));
		}
		return result;
	}

	/** 更换Row的Schema数据结构，如果对不上则爆出异常*/
	public static FlinkTableRow createWithNewSchema(FlinkTableSchema schema, FlinkTableRow row)
	{
		if (row.getSchema().equals(schema))
		{
			return row;
		} else
		{
			FlinkTableRow result = new FlinkTableRow(schema, row.getRowKind());
			for (String column : schema.getRowColumnNames())
			{
				if (row.getSchema().getRowColumnNames().contains(column))
				{
					/** 需要判断一下两个Schema的数据类型是是否一致，先偷个懒*/
					result.setRowValueOfColumn(column, row.getRowValueOfColumn(column));
				} else
				{
					// 数据库中的列在row中没有
					if (schema.getNotNullColumnNames().contains(column))
					{
						// 但是数据库中必须非空
						System.out.println("\033[1;31m" + "传入的流数据源中不包含列 " + column + " 的数据" + "\033[0m");
						throw new IllegalArgumentException("传入的流数据源中不包含列 " + column + " 的数据");
					} else
					{
						// 插入一个null
						result.setRowValueOfColumn(column, null);
					}
				}
			}
			return result;
		}
	}
}
