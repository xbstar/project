package com.xbstar.http;

import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

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

	/** 打印算子警告输出提示*/
	public static void printWarning(String message)
	{
		System.out.println("\033[1;33m" + message + "\033[0m");
	}
}
