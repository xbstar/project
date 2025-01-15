package com.xbstar.spring;

import com.alibaba.fastjson.JSONObject;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.*;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.UnknownHttpStatusCodeException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Map;

public class XBStarHttp
{
	/** 用户可配置默认地址和授权信息*/
	public static String ServerAddress = "";
	public static String Authorization = null;

	static // 配置忽略证书验证
	{
		try
		{
			SSLContext sc = SSLContext.getInstance("TLS");
			sc.init(null, new TrustManager[] { new X509TrustManager()
			{
				@Override
				public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException
				{
				}

				@Override
				public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException
				{
				}

				@Override
				public X509Certificate[] getAcceptedIssuers()
				{
					return new X509Certificate[0];
				}
			} }, new SecureRandom());
			SSLContext.setDefault(sc);
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	/** 发起HTTPGet请求获取资源信息*/
	public static ResponseEntity<String> httpGet(String url)
	{
		return httpRequest(url, HttpMethod.GET, new HttpHeaders(), "", String.class, true);
	}

	/** 发起HTTPGet请求获取资源,即使不存在也不报错*/
	public static ResponseEntity<String> httpCheck(String url)
	{
		return httpRequest(url, HttpMethod.GET, new HttpHeaders(), "", String.class, false);
	}

	/** 发起HTTPGet请求，通过X-check-Exist拓展获取资源存在状态*/
	public static boolean httpExist(String url)
	{
		ResponseEntity<String> response = httpRequest(url, HttpMethod.GET, new HttpHeaders(), "", String.class, false);
		return response.getStatusCode() == HttpStatus.OK ? true : false;
	}

	/** 发起HTTPPost请求修改或者创建资源*/
	public static ResponseEntity<String> httpUpdate(String url, HttpMethod method, Object body)
	{
		return httpRequest(url, method, new HttpHeaders(), body, String.class, true);
	}

	/** 复杂HTTP请求*/
	public static <T> ResponseEntity<T> httpRequest(String requestURL, HttpMethod requestMethod, HttpHeaders requestHeader, Object requestBody, Class<T> responseClass, boolean errorWhenNotFound)
	{
		if (Authorization != null)
		{
			requestHeader.set("Authorization", Authorization);
		}
		requestBody = requestBody == null ? "" : requestBody;
		if (requestBody instanceof Map || requestBody instanceof JSONObject)
		{
			requestHeader.set("Content-Type", "application/json");
		} else
		{
			requestHeader.set("Content-Type", "text/plain;charset=UTF-8");
		}
		HttpHeaders responseHeader = new HttpHeaders();
		HttpStatus responseStatus = null;
		Object responseContent;
		requestURL = requestURL.startsWith("/") ? ServerAddress + requestURL : requestURL;
		try
		{
			// 发起HTTP请求
			RestTemplateBuilder request = new RestTemplateBuilder().setConnectTimeout(Duration.ofSeconds(6)).setReadTimeout(Duration.ofSeconds(30));
			ResponseEntity<?> response = request.build().exchange(requestURL, requestMethod, new HttpEntity(requestBody, requestHeader), responseClass);
			responseStatus = response.getStatusCode();
			responseContent = response.getBody();
			responseHeader = response.getHeaders();
		} catch (HttpServerErrorException | HttpClientErrorException e)
		{
			responseContent = e.getResponseBodyAsString().replace("\n", "");
			responseStatus = e.getStatusCode();
		} catch (UnknownHttpStatusCodeException e)
		{
			responseContent = e.getResponseBodyAsString() + "(Code:" + e.getRawStatusCode() + ")";
			responseStatus = HttpStatus.FAILED_DEPENDENCY;
		} catch (Exception e)
		{
			responseStatus = HttpStatus.REQUEST_TIMEOUT;
			responseContent = e.getMessage();
		}
		if (!(responseStatus == HttpStatus.OK || responseStatus == HttpStatus.CREATED || responseStatus == HttpStatus.ACCEPTED) && (errorWhenNotFound || responseStatus != HttpStatus.NOT_FOUND))
		{
			String proxy = System.getProperty("http.proxyHost");
			proxy = proxy == null ? "" : "代理(" + proxy + ":" + System.getProperty("http.proxyPort") + ")";
			XBStarUtils.printError(proxy + "请求" + requestURL + "失败(" + responseStatus + ")：" + responseContent);
		}
		return new ResponseEntity(responseContent, responseHeader, responseStatus);
	}
}
