package com.xbstar.spring;

public class XBStarException extends RuntimeException
{
	public int errorCode;
	public String errorMessage;

	public XBStarException(int errorCode, String errorMessage)
	{
		super(errorMessage);
		this.errorCode = errorCode;
		this.errorMessage = errorMessage;
	}
}
