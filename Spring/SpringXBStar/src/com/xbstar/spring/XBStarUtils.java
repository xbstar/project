package com.xbstar.spring;

public class XBStarUtils
{
	/** 打印算子详细输出提示*/
	public static void printVerbose(String message)
	{
		System.out.println("\033[1;37m" + message + "\033[0m");
	}

	/** 打印算子错误输出提示*/
	public static void printError(String message)
	{
		System.out.println("\033[1;31m" + message + "\033[0m");
	}
}
