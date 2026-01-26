package com.xbstar.types;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Collections;
import java.util.Set;

public class Type
{
	public String DebeziumType;
	public String apache;
	public Integer length; //不一定有看数据类型
	public Integer precision;
	public Integer scale;

	public static Type fromSQLiteType(String sqliteType)
	{
		Type type = new Type();
		if ("TEXT".equals(sqliteType)) type.apache = "org.apache.flink.table.types.logical.VarCharType";
		else if ("INT".equals(sqliteType) || "INTEGER".equals(sqliteType)) type.apache = "org.apache.flink.table.types.logical.IntType";
		else if ("REAL".equals(sqliteType)) type.apache = "org.apache.flink.table.types.logical.FloatType";
		else if ("BLOB".equals(sqliteType)) type.apache = "org.apache.flink.table.types.logical.BinaryType";
		else type.apache = "org.apache.flink.table.types.logical.VarCharType";
		return type;
	}

	public static Type fromMySQLType(String mysqlType)
	{
		Type type = new Type();
		switch (mysqlType)
		{
			case "tinyint":
			{
				type.apache = "org.apache.flink.table.types.logical.TinyIntType";
				break;
			}
			case "smallint":
			{
				type.apache = "org.apache.flink.table.types.logical.SmallIntType";
				break;
			}
			case "bigint":
			{
				type.apache = "org.apache.flink.table.types.logical.BigIntType";
				break;
			}
			case "int":
			{
				type.apache = "org.apache.flink.table.types.logical.IntType";
				break;
			}
			case "double":
			{
				type.apache = "org.apache.flink.table.types.logical.DoubleType";
				break;
			}
			case "date":
			{
				type.apache = "org.apache.flink.table.types.logical.DateType";
				break;
			}
			case "datetime":
			{
				type.apache = "org.apache.flink.table.types.logical.TimestampType";
				break;
			}
			case "bit":
			case "enum":
			case "text":
			case "varchar":
			case "varbinary":
			{
				return fromMySQLType(mysqlType, 0);
			}
			case "decimal":
			{
				return fromMySQLType(mysqlType, 3, 2);
			}
			default:
			{
				throw new RuntimeException("没有处理的MySQL数据类型:" + mysqlType);
			}
		}
		return type;
	}

	public static Type fromMySQLType(String mysqlType, int length)
	{
		Type type = new Type();
		type.length = length;
		switch (mysqlType)
		{
			case "bit":
			{
				if (length == 1) type.apache = "org.apache.flink.table.types.logical.BooleanType";
				else type.apache = "org.apache.flink.table.types.logical.BinaryType";
				break;
			}
			case "decimal":
			{
				return fromMySQLType(mysqlType, length, 2);
			}
			case "enum":
			case "text":
			case "varchar":
			{
				type.apache = "org.apache.flink.table.types.logical.VarCharType";
				break;
			}
			case "varbinary":
			{
				type.apache = "org.apache.flink.table.types.logical.VarBinaryType";
				break;
			}
			default:
			{
				return fromMySQLType(mysqlType);
			}
		}
		return type;
	}

	public static Type fromMySQLType(String mysqlType, int precision, int scale)
	{
		if ("decimal".equals(mysqlType))
		{
			Type type = new Type();
			type.apache = "org.apache.flink.table.types.logical.DecimalType";
			type.precision = precision;
			type.scale = scale;
			return type;
		}
		return fromMySQLType(mysqlType, precision);
	}

	public String toSQLiteType()
	{
		switch (this.apache)
		{
			case "org.apache.flink.table.types.logical.BooleanType":
			{
				// SQLite 没有 BOOLEAN 存储类型。boolean 本质上存为 INTEGER（0 / 1）
				return "integer";
			}
			case "org.apache.flink.table.types.logical.TinyIntType":
			case "org.apache.flink.table.types.logical.SmallIntType":
			case "org.apache.flink.table.types.logical.BigIntType":
			case "org.apache.flink.table.types.logical.IntType":
			{
				return "integer";
			}
			case "org.apache.flink.table.types.logical.DoubleType":
			case "org.apache.flink.table.types.logical.FloatType":
			{
				return "real";
			}
			case "org.apache.flink.table.types.logical.VarCharType":
			{
				return "text";
			}
			case "org.apache.flink.table.types.logical.DateType":
			case "org.apache.flink.table.types.logical.TimestampType":
			{
				// 时间yyyy-MM-dd会被当成字符串来存储
				return "text";
			}
			case "org.apache.flink.table.types.logical.VarBinaryType":
			case "org.apache.flink.table.types.logical.BinaryType":
			{
				return "blob";
			}
			default:
			{
				throw new RuntimeException(apache + "暂时没有处理");
			}
		}
	}

	public String toMySQLType()
	{
		switch (this.apache)
		{
			case "org.apache.flink.table.types.logical.BooleanType":
			{
				return "bit(1)";
			}
			case "org.apache.flink.table.types.logical.TinyIntType":
			{
				return "tinyint";
			}
			case "org.apache.flink.table.types.logical.SmallIntType":
			{
				return "smallint";
			}
			case "org.apache.flink.table.types.logical.BigIntType":
			{
				return "bigint";
			}
			case "org.apache.flink.table.types.logical.IntType":
			{
				return "int";
			}
			case "org.apache.flink.table.types.logical.FloatType":
			{
				return "float";
			}
			case "org.apache.flink.table.types.logical.DoubleType":
			{
				return "double";
			}
			case "org.apache.flink.table.types.logical.DecimalType":
			{
				return "decimal(" + precision + "," + scale + ")";
			}
			case "org.apache.flink.table.types.logical.VarCharType":
			{
				return length == null || length == 0 ? "text" : "varchar(" + length + ")";
			}
			case "org.apache.flink.table.types.logical.VarBinaryType":
			{
				return "varbinary(" + length + ")";
			}
			case "org.apache.flink.table.types.logical.TimestampType":
			{
				return "datetime";
			}
			case "org.apache.flink.table.types.logical.DateType":
			{
				return "date";
			}
			case "org.apache.flink.table.types.logical.BinaryType":
			{
				return "bit(" + length + ")";
			}
			default:
			{
				throw new RuntimeException(apache + "暂时没有处理");
			}
		}
	}

	public String toFlinkType()
	{
		switch (this.apache)
		{
			case "org.apache.flink.table.types.logical.TinyIntType":
			{
				return "TINYINT";
			}
			case "org.apache.flink.table.types.logical.SmallIntType":
			{
				return "SMALLINT";
			}
			case "org.apache.flink.table.types.logical.BigIntType":
			{
				return "BIGINT";
			}
			case "org.apache.flink.table.types.logical.IntType":
			{
				return "INT";
			}
			case "org.apache.flink.table.types.logical.BooleanType":
			{
				return "BOOLEAN";
			}
			case "org.apache.flink.table.types.logical.DoubleType":
			{
				return "DOUBLE";
			}
			case "org.apache.flink.table.types.logical.CharType":
			{
				return "CHAR(" + length + ")";
			}
			case "org.apache.flink.table.types.logical.VarCharType":
			{
				return "VARCHAR(" + length + ")";
			}
			case "org.apache.flink.table.types.logical.VarBinaryType":
			{
				return "BINARY(" + (length * 8) + ")";
			}
			case "org.apache.flink.table.types.logical.BinaryType":
			{
				return "BINARY(" + length + ")";
			}
			case "org.apache.flink.table.types.logical.DateType":
			{
				return "DATE";
			}
			case "org.apache.flink.table.types.logical.TimestampType":
			{
				return "TIMESTAMP(" + length + ")";
			}
			case "org.apache.flink.table.types.logical.FloatType":
			{
				return "FLOAT";
			}
			case "org.apache.flink.table.types.logical.DecimalType":
			{
				return "DECIMAL(" + precision + "," + scale + ")";
			}
			default:
			{
				throw new RuntimeException(apache + "暂时没有处理");
			}
		}
	}

	public LogicalTypeRoot toFlinkRootType()
	{
		return this.toFlinkDataType().getLogicalType().getTypeRoot();
	}

	public DataType toFlinkDataType()
	{
		DataType result = DataTypes.NULL();
		switch (this.apache)
		{
			case "org.apache.flink.table.types.logical.TinyIntType":
			{
				result = DataTypes.TINYINT();
				break;
			}
			case "org.apache.flink.table.types.logical.SmallIntType":
			{
				result = DataTypes.SMALLINT();
				break;
			}
			case "org.apache.flink.table.types.logical.BigIntType":
			{
				result = DataTypes.BIGINT();
				break;
			}
			case "org.apache.flink.table.types.logical.IntType":
			{
				result = DataTypes.INT();
				break;
			}
			case "org.apache.flink.table.types.logical.BooleanType":
			{
				result = DataTypes.BOOLEAN();
				break;
			}
			case "org.apache.flink.table.types.logical.DoubleType":
			{
				result = DataTypes.DOUBLE();
				break;
			}
			case "org.apache.flink.table.types.logical.CharType":
			{
				result = DataTypes.CHAR(length);
				break;
			}
			case "org.apache.flink.table.types.logical.VarCharType":
			{
				result = DataTypes.VARCHAR(length);
				break;
			}
			case "org.apache.flink.table.types.logical.VarBinaryType":
			{
				result = DataTypes.VARBINARY(length);
				break;
			}
			case "org.apache.flink.table.types.logical.BinaryType":
			{
				result = DataTypes.BINARY(length);
				break;
			}
			case "org.apache.flink.table.types.logical.DateType":
			{
				result = DataTypes.DATE();
				break;
			}
			case "org.apache.flink.table.types.logical.TimestampType":
			{
				// 注意这里RowdataValue给过来的是TimestampData，不要用3否则转换错误
				result = DataTypes.TIMESTAMP();
				break;
			}
			case "org.apache.flink.table.types.logical.FloatType":
			{
				result = DataTypes.FLOAT();
				break;
			}
			case "org.apache.flink.table.types.logical.DecimalType":
			{
				result = DataTypes.DECIMAL(precision, scale);
				break;
			}
			default:
			{
				throw new RuntimeException(apache + "暂时没有处理");
			}
		}
		return result;
	}

	@Override
	public String toString()
	{
		String type = this.apache.substring(apache.lastIndexOf(".") + 1, apache.length());
		if(Set.of("FloatType","IntType","BinaryType").contains(type)) type += "\t";
		if (length == null && precision == null && scale == null) return type;
		else return "{type:" + type + " length:" + length + " precision:" + precision + " scale:" + scale + "}";
	}
}
