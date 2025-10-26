package com.xbstar.chenyiming;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DMLogSQLParser
{
	public static DMLBean parseSql(String sql)
	{
		if (sql.toLowerCase().startsWith("insert"))
		{
			return parseInsert(sql);
		}
		else if (sql.toLowerCase().startsWith("update"))
		{
			return parseUpdate(sql);
		}
		else if (sql.toLowerCase().startsWith("delete"))
		{
			return parseDelete(sql);
		}
		return null;
	}

	/**
	 * delete
	 */
	private static DMLBean parseDelete(String sql)
	{
		DMLBean dmlBean = new DMLBean();
		// op属性
		dmlBean.setOp("DELETE");
		// 创建Delete对象
		Delete delete = null;
		try
		{
			delete = (Delete) CCJSqlParserUtil.parse(sql);
		} catch (JSQLParserException e)
		{
			e.printStackTrace();
		}
		// table属性
		dmlBean.setTable(delete.getTable().getName().replace("\"", ""));
		// key属性
		dmlBean.setKeys(getKeyMap(delete.getWhere()));

		return dmlBean;
	}

	/**
	 * update
	 */
	private static DMLBean parseUpdate(String sql)
	{
		DMLBean dmlBean = new DMLBean();
		// op属性
		dmlBean.setOp("UPDATE");
		// 创建Update对象
		Update update = null;
		try
		{
			update = (Update) CCJSqlParserUtil.parse(sql);
		} catch (JSQLParserException e)
		{
			e.printStackTrace();
		}
		// table属性
		dmlBean.setTable(update.getTable().getName().replace("\"", ""));
		// after属性
		dmlBean.setAfter(getColumnValueMap(update));
		// key属性
		dmlBean.setKeys(getKeyMap(update.getWhere()));

		return dmlBean;
	}

	/**
	 * insert
	 */
	private static DMLBean parseInsert(String sql)
	{
		DMLBean dmlBean = new DMLBean();
		// op属性
		dmlBean.setOp("INSERT");
		// 创建Insert对象
		Insert insert = null;
		try
		{
			insert = (Insert) CCJSqlParserUtil.parse(sql);
		} catch (JSQLParserException e)
		{
			e.printStackTrace();
		}
		// table属性
		dmlBean.setTable(insert.getTable().getName().replace("\"", ""));
		// after属性
		dmlBean.setAfter(getColumnValueMap(insert.getColumns(), ((ExpressionList) insert.getItemsList()).getExpressions()));

		return dmlBean;
	}

	/**
	 * 处理 sql中column和value 成map对象
	 */
	private static Map<String, String> getColumnValueMap(List<Column> columnList, List<Expression> itemsList)
	{
		Map<String, String> map = new HashMap<>();
		for (int i = 0; i < itemsList.size(); i++)
		{
			Expression expression = itemsList.get(i);
			String columnName = columnList.get(i).getColumnName().replace("\"", "");
			map.put(columnName, getExpressionStringValue(expression));
		}
		return map;
	}

	/**
	 * 处理 sql中column和value 成map对象 (API中的UPDATE有bug，所以自己处理一下)
	 */
	private static Map<String, String> getColumnValueMap(Update update)
	{
		Map<String, String> map = new HashMap<>();
		List<UpdateSet> updateSets = update.getUpdateSets();
		for (UpdateSet updateSet : updateSets)
		{
			map.put(updateSet.getColumns().get(0).getColumnName().replace("\"", ""), getExpressionStringValue(updateSet.getExpressions().get(0)));
		}
		return map;
	}

	/**
	 * 处理 sql中where 成map对象
	 * tip1 : 联合主键只处理了2个的情况
	 */
	private static Map<String, String> getKeyMap(Expression expression)
	{
		Map<String, String> map = new HashMap<>();
		if (expression instanceof EqualsTo)
		{
			EqualsTo equalsTo = (EqualsTo) expression;
			map.put(equalsTo.getLeftExpression().toString().replace("\"", ""), getExpressionStringValue(equalsTo.getRightExpression(), false));
			return map;
		}
		else if (expression instanceof AndExpression)
		{
			AndExpression andExpression = (AndExpression) expression;
			EqualsTo leftEqualsTo = (EqualsTo) andExpression.getLeftExpression();
			map.put(leftEqualsTo.getLeftExpression().toString().replace("\"", ""), getExpressionStringValue(leftEqualsTo
					.getRightExpression(), false));
			EqualsTo rightEqualsTo = (EqualsTo) andExpression.getRightExpression();
			map.put(rightEqualsTo.getLeftExpression().toString().replace("\"", ""), getExpressionStringValue(rightEqualsTo
					.getRightExpression(), false));
			return map;
		}
		return null;
	}

	/**
	 * 获取Expression对象的string数值
	 * */
	private static String getExpressionStringValue(Expression expression)
	{
		return getExpressionStringValue(expression, true);
	}

	/**
	 * 获取Expression对象的string数值
	 * @param isValues : 达梦log中 VALUES数据的单引号(')会被处理成两个('')，用来判断是否需要处理
	 * */
	private static String getExpressionStringValue(Expression expression, boolean isValues)
	{
		if (expression instanceof StringValue)
		{
			StringValue stringValue = (StringValue) expression;
			String value = stringValue.getValue();
			if (isValues)
			{
				value = value.replace("''", "'");
			}
			return value;
		}
		else if (expression instanceof LongValue)
		{
			LongValue longValue = (LongValue) expression;
			return longValue.getStringValue();
		}
		return null;
	}
}
