package com.xbstar.utils;

public class PrintUtils
{
	public static void printVerbose(String message)
	{
		System.out.println("\033[1;37m" + message + "\033[0m");
	}

	public static void printError(String message)
	{
		System.out.println("\033[1;31m" + message + "\033[0m");
	}

	public static void printWarning(String message)
	{
		System.out.println("\033[1;33m" + message + "\033[0m");
	}

	public static void printGreen(String message)
	{
		System.out.println("\033[1;32m" + message + "\033[0m");
	}

	public static void printInfo(String message) {System.out.println("\033[1;94m" + message + "\033[0m");}

	public static void printImportant(String message)
	{
		System.out.println("\033[1;35m" + message + "\033[0m");
	}
}
