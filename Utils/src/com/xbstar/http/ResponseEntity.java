package com.xbstar.http;

import com.xbstar.http.HttpStatus;

import java.util.Collections;
import java.util.Map;

public class ResponseEntity
{
	public HttpStatus status;
	public Map<String, String> headers;
	public String content;

	public ResponseEntity(HttpStatus status)
	{
		this("", status);
	}

	public ResponseEntity(String content, HttpStatus status)
	{
		this(content, Collections.emptyMap(), status);
	}

	public ResponseEntity(String content, Map<String, String> headers, HttpStatus status)
	{
		this.status = status;
		this.headers = headers;
		this.content = content;
	}

	public HttpStatus getStatusCode()
	{
		return status;
	}

	public int getStatusCodeValue()
	{
		return status.value();
	}

	public String getBody()
	{
		return content;
	}
}
