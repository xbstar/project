package com.xbstar.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class EnvUtils
{
	public static <T> T getConfigFromEnvironment(Class<T> cla, String key)
	{
		// 首先尝试从环境变量中读取
		String config = System.getenv(key);
		// 然后尝试从application.properties文件中读取
		if (config == null)
		{
			try (InputStream input = EnvUtils.class.getClassLoader().getResourceAsStream("application.properties");)
			{
				Properties props = new Properties();
				props.load(input);
				config = props.getProperty(key);
			} catch (IOException e)
			{
			}
		}
		if (config == null) return null;// 确实没有返回null
		else// 如果获取到了就进行类型转换
		{
			switch (cla.getSimpleName())
			{
				case "String":
				{
					return (T) config;
				}
				case "Integer":
				{
					return (T) Integer.valueOf(config);
				}
				default:
				{
					throw new RuntimeException("[EnvUtils]不支持的返回值类型" + cla.getName());
				}
			}
		}
	}
}
