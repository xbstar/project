package com.xbstar.utils;

public class ConvertUtils
{
	public static String toCamelCase(String input) //将字符串转换为大写驼峰命名
	{
		if (input == null || input.isEmpty())
		{
			return input;
		}
		// 移除开头和结尾的特殊字符
		input = input.trim();
		// 使用正则分割多种分隔符
		String[] parts = input.split("[_\\-\\.\\s]+");
		StringBuilder result = new StringBuilder();
		for (String part : parts)
		{
			if (part.isEmpty()) continue;
			if ("db".equals(part.toLowerCase())) result.append("DB");
			else if ("mt".equals(part.toLowerCase())) result.append("MT");
			else result.append(Character.toUpperCase(part.charAt(0))).append(part.substring(1).toLowerCase());// 首字母大写，其余小写
		}
		return result.toString();
	}
}
