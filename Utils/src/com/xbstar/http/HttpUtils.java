package com.xbstar.http;

import com.alibaba.fastjson.JSONObject;
import com.xbstar.utils.PrintUtils;
import okhttp3.*;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Method;
import java.net.*;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class HttpUtils
{
	/** XBStarHttp配置参数*/
	public static String Authorization = null; //默认HTTP请求授权信息
	public static String ServerAddress = null; //默认的服务器地址前缀
	public static Boolean OnErrorPrint = true; //默认会打印HTTP请求的错误
	public static String PostFileName = null; //如果使用Post上传给的是byte[]通过这个变量指定文件名，不指定则是byte[]的hashcode
	public static String ServerProxy = setUseProxy();  //记录是否用HTTP或者Socks5代理
	public static Map<String, Boolean> ProxyMap = new HashMap<>(); //用户可以详细的配置是否使用代理发送请求

	public static String setUseProxy() //启用HTTP请求代理，如果环境变量中没有配置代理则无效
	{
		String socksProxyHost = System.getProperty("socksProxyHost");
		String socksProxyPort = System.getProperty("socksProxyPort");
		String httpProxyHost = System.getProperty("http.proxyHost");
		String httpProxyPort = System.getProperty("http.proxyPort");
		if (socksProxyHost != null) ServerProxy = "socks5://" + socksProxyHost + ":" + socksProxyPort;
		if (httpProxyHost != null) ServerProxy = "http://" + httpProxyHost + ":" + httpProxyPort;
		if (ServerProxy != null) PrintUtils.printVerbose("[XBStarHTTP]启用了TCP代理：" + ServerProxy);
		return ServerProxy;
	}

	public static void setAuthorization(String auth)
	{
		PrintUtils.printVerbose("[XBStarHTTP]开启了HTTP授权：" + auth);
		Authorization = auth;
	}

	public static void setServerAddress(String address)
	{
		PrintUtils.printVerbose("[XBStarHTTP]设置了默认访问地址" + address);
		ServerAddress = address;
	}

	public static void setOnErrorPrint(Boolean onErrorPrint)
	{
		PrintUtils.printVerbose("[XBStarHTTP]设置了请求错误信息打印：" + String.valueOf(onErrorPrint).toUpperCase());
		OnErrorPrint = onErrorPrint;
	}

	public static OkHttpClient client(String url, Integer timeout)
	{
		/** 配置OKHTTP客户端的请求调用处理和异常捕获*/
		OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder().addInterceptor(chain -> //配置请求拦截器
		{
			if (timeout != null) chain = chain.withConnectTimeout(timeout, TimeUnit.SECONDS);
			Request originalRequest = chain.request();
			try
			{
				return chain.proceed(originalRequest);
			} catch (UnknownHostException e)
			{
				ResponseBody body = ResponseBody.create(MediaType.parse("text/plain;charset=UTF-8"), "域名解析失败");
				return new Response.Builder().protocol(Protocol.HTTP_1_1).request(originalRequest).code(410).body(body).message("域名无法解析").build();
			} catch (SocketException | SocketTimeoutException e)
			{
				ResponseBody body = ResponseBody.create(MediaType.parse("text/plain;charset=UTF-8"), originalRequest.url() + "联不通");
				return new Response.Builder().protocol(Protocol.HTTP_1_1).request(originalRequest).code(504).body(body).message("服务器访问超时").build();
			} catch (InterruptedIOException e)
			{
				ResponseBody body = ResponseBody.create(MediaType.parse("text/plain;charset=UTF-8"), "IO被中断");
				return new Response.Builder().protocol(Protocol.HTTP_1_1).request(originalRequest).code(302).body(body).message("IO操作被中断").build();
			} catch (Exception e)
			{
				e.printStackTrace();
				ResponseBody body = ResponseBody.create(MediaType.parse("text/plain;charset=UTF-8"), e.getMessage() == null ? "出现异常" : e.getMessage());
				return new Response.Builder().protocol(Protocol.HTTP_1_1).request(originalRequest).code(0).body(body).message("出现了没有见过的异常").build();
			}
		});
		/** 配置OKHTTP客户端使用代理*/
		if (ServerProxy != null) //环境中配置了代理
		{
			boolean proxy = true;
			for (String address : ProxyMap.keySet()) if (url.startsWith(address)) proxy = ProxyMap.get(address);
			if (proxy)
			{
				//用户没有配置或者配置为需要代理
				URI proxyURI = URI.create(ServerProxy);
				switch (proxyURI.getScheme())
				{
					case "socks5":
					{
						clientBuilder.proxy(new Proxy(Proxy.Type.SOCKS, new InetSocketAddress(proxyURI.getHost(), Integer.valueOf(proxyURI.getPort()))));
						break;
					}
					case "com/xbstar/http":
					{
						clientBuilder.proxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyURI.getHost(), Integer.valueOf(proxyURI.getPort()))));
						break;
					}
					default:
					{
						//其他代理类型暂时不支持
						throw new RuntimeException("其他代理类型:" + proxyURI.getScheme() + "暂时不支持");
					}
				}
			}
			else
			{
				clientBuilder.proxy(Proxy.NO_PROXY);
			}
		}
		/** 配置忽略自签名的HTTPS证书验证*/
		try
		{
			TrustManager[] trustManagers = { new X509TrustManager()
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
			} }; // HTTPS的信任中心配置
			SSLContext sc = SSLContext.getInstance("TLS");
			sc.init(null, trustManagers, new SecureRandom());
			clientBuilder.sslSocketFactory(sc.getSocketFactory(), (X509TrustManager) trustManagers[0]);
			SSLContext.setDefault(sc);
		} catch (Exception e)
		{
			PrintUtils.printError("OKHTTP初始化遇到问题：");
			throw new RuntimeException(e);
		}
		// 构造客户端
		clientBuilder.readTimeout(30, TimeUnit.SECONDS);
		clientBuilder.writeTimeout(30, TimeUnit.SECONDS);
		clientBuilder.connectTimeout(30, TimeUnit.SECONDS);
		return clientBuilder.build();
	}

	/** 发起HTTPGet请求获取资源信息*/
	public static ResponseEntity httpGet(String url)
	{
		return httpRequest(url, HttpMethod.GET, new LinkedHashMap<>(), "", null);
	}

	/** 发起HTTPGet请求获取资源,即使不存在也不报错*/
	public static ResponseEntity httpCheck(String url)
	{
		Boolean temp = OnErrorPrint;
		OnErrorPrint = false;
		ResponseEntity result = httpRequest(url, HttpMethod.GET, new LinkedHashMap<>(), "", null);
		OnErrorPrint = temp;
		return result;
	}

	/** 发起HTTPGet请求，通过X-check-Exist拓展获取资源存在状态*/
	public static boolean httpExist(String url)
	{
		ResponseEntity response = httpRequest(url, HttpMethod.GET, new LinkedHashMap<>(), "", null);
		return response.getStatusCode() == HttpStatus.OK ? true : false;
	}

	/** 发起HTTPPost请求修改或者创建资源*/
	public static ResponseEntity httpUpdate(String url, HttpMethod method, Object body)
	{
		return httpRequest(url, method, new LinkedHashMap<>(), body, null);
	}

	public static ResponseEntity httpUpdate(String url, HttpMethod method, Object body, Integer timeout)
	{
		return httpRequest(url, method, new LinkedHashMap<>(), body, timeout);
	}

	/** 复杂HTTP请求*/
	public static ResponseEntity httpRequest(String requestURL, HttpMethod requestMethod, Map<String,String> requestHeader, Object requestBody, Integer timeout)
	{
		// 构造请求地址
		requestURL = requestURL.startsWith("/") ? ServerAddress + requestURL : requestURL;
		Request.Builder request = new Request.Builder().url(requestURL);
		requestBody = requestBody == null ? "" : requestBody;
		// 构造请求Header
		if (Authorization != null) requestHeader.put("Authorization", Authorization);// 初始化认证授权
		for (String key : requestHeader.keySet())
		{
			request.header(key, requestHeader.get(key));
		}
		// 预转换一些子当以类型
		if ("com.xbstar.deliver.Deliver".equals(requestBody.getClass().getName()))
		{
			System.out.println("命中了Deliver");
			try
			{
				Method toJSON = requestBody.getClass().getDeclaredMethod("toJSON");
				requestBody = toJSON.invoke(requestBody); //将requestBody转为JSON
			} catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}
		// 构造RequestBody
		if (requestBody instanceof JSONObject)
		{
			if (requestMethod == HttpMethod.PATCH)
			{
				requestBody = RequestBody.create(MediaType.parse("application/merge-patch+json"), requestBody.toString());
			}
			else
			{
				requestBody = RequestBody.create(MediaType.parse("application/json"), requestBody.toString());
			}
		}
		else if (requestBody instanceof Map)
		{
			Map<String, Object> map = (Map<String, Object>) requestBody;
			FormBody.Builder builder = new FormBody.Builder();
			for (String key : map.keySet())
			{
				Object val = map.get(key);
				if (val == null) continue;
				if (val instanceof Collection) for (Object curVal : ((Collection) val)) builder.add(key, curVal.toString());
				else builder.add(key, val.toString());
			}
			requestBody = builder.build();
		}
		else if (requestBody instanceof byte[])
		{
			RequestBody fileBody = RequestBody.create(MediaType.parse("application/octet-stream"), (byte[]) requestBody);
			String fileName = PostFileName == null ? String.valueOf(requestBody.hashCode()) : PostFileName;
			requestBody = new MultipartBody.Builder().setType(MultipartBody.FORM).addFormDataPart("file", fileName, fileBody).build();
		}
		else
		{
			requestBody = RequestBody.create(MediaType.parse("text/plain;charset=UTF-8"), requestBody.toString());
		}
		// 构造Method和body
		if (requestMethod == HttpMethod.GET) request.get();
		if (requestMethod == HttpMethod.POST) request.post((RequestBody) requestBody);
		if (requestMethod == HttpMethod.PATCH) request.patch((RequestBody) requestBody);
		if (requestMethod == HttpMethod.PUT) request.put((RequestBody) requestBody);
		if (requestMethod == HttpMethod.DELETE) request.delete((RequestBody) requestBody);
		// 发起HTTP请求
		Response httpResponse = null;
		String responseContent = null;
		try
		{
			// 通过OKHTTP发起请求
			httpResponse = httpResponse = client(requestURL, timeout).newCall(request.build()).execute();
			responseContent = httpResponse.body().string();// 首先解析Content
		} catch (IOException e)
		{
			// 上面的OkHttp客户端应该已经捕获了所有异常，所以这里肯定不会执行
			e.printStackTrace();
		}
		// 解析返回码，返回0代表OKHttp捕获到异常了
		HttpStatus responseStatus = HttpStatus.valueOf(httpResponse.code());
		//成功获得响应，解析Header
		Map<String, String> responseHeader = new LinkedHashMap<>();
		for (String key : httpResponse.headers().names())
		{
			for (String val : httpResponse.headers(key))
			{
				responseHeader.put(key, val);
			}
		}
		// 请求返回结果日志打印处理
		if (!(httpResponse.code() == 200 || httpResponse.code() == 201 || httpResponse.code() == 202) && OnErrorPrint)
		{
			String proxy = ServerProxy == null ? "" : "代理(" + ServerProxy + ")";
			PrintUtils.printError(proxy + "请求" + requestURL + "失败(" + httpResponse.code() + ")：" + httpResponse.message());
		}
		return new ResponseEntity(responseContent, responseHeader, responseStatus);
	}

	public static WebSocket httpWebSocket(String socketURL, WebSocketListener handler)
	{
		// 获得正确的请求地址
		socketURL = socketURL.startsWith("/") && ServerAddress != null ? "ws" + ServerAddress.substring(ServerAddress.indexOf(":")) + socketURL : socketURL;
		//发起socket请求
		Request.Builder request = new Request.Builder().url(socketURL);
		if (!"".equals(ServerAddress)) request.header("Origin", ServerAddress);
		if (!"".equals(Authorization)) request.header("Authorization", Authorization);
		try
		{
			return client(socketURL, null).newWebSocket(request.build(), handler);
		} catch (Exception e)
		{
			return null;
		}
	}

	/** 将域名地址转换为谱同地址*/
	public static String resolveAddress(String url)
	{
		URI uri = URI.create(url);
		if (uri.getScheme() == null) throw new RuntimeException("[XBStarHttp]传入的地址路径不合法:" + url);
		return uri.getScheme() + "://" + resolveIP(uri.getHost()) + ":" + uri.getPort() + (uri.getPath() == null ? "" : "/" + uri.getPath());
	}

	/** 自动选择解析策略并解析域名*/
	public static String resolveIP(String url)
	{
		if (ServerProxy == null) return resolveIPLocally(url); //没有代理只能本地解析
		Boolean proxy = ProxyMap.get(URI.create(url).getScheme() == null ? "http://" + url : url);
		if (proxy == null || proxy) return resolveIPCluster(url); //要代理用集群域名服务器解析
		else return resolveIPLocally(url); //不代理直接解析
	}

	/* 将domain在集群上解析，解析不了则返回null*/
	public static String resolveIPCluster(String url)
	{
		URI uri = URI.create(url);
		if (uri.getScheme() == null) uri = URI.create("http://" + url); //用户可能直接给的domain
		String domain = uri.getHost();
		if (isIPv4(domain)) return domain;
		String k8sApi = "http://ks-apiserver.kubesphere-system.svc.cluster.local/api/v1/namespaces/kube-system/services/coredns";
		Request.Builder request = new Request.Builder().url(k8sApi);
		if (Authorization != null) request.header("Authorization", Authorization);
		request.get();
		try
		{
			Response response = client("", null).newCall(request.build()).execute();
			if (response.code() != 200)
			{
				PrintUtils.printWarning("[XBStarHttp]通过K8SApi获取CoreDNS失败(" + response.code() + "):" + response.message());
				return "出错了";
			}
			String coreDNSIP = JSONObject.parseObject(response.body().string()).getJSONObject("spec").getString("clusterIP");
			return DnsResolver.resolve(domain, coreDNSIP, 53);
		} catch (Exception e)
		{
			PrintUtils.printWarning("[XBStarHttp]通过K8SApi请求CoreDNS失败:" + e.getMessage());
			return "出错了";
		}
	}

	/* 将domain用本地域名系统解析,解析不了返回null*/
	public static String resolveIPLocally(String url)
	{
		URI uri = URI.create(url);
		if (uri.getScheme() == null) uri = URI.create("http://" + url); //用户可能直接给的domain
		String domain = uri.getHost();
		try
		{
			if (isIPv4(domain)) return domain;
			return InetAddress.getByName(domain).getHostAddress();
		} catch (Exception e)
		{
			PrintUtils.printWarning("[XBStarHttp]本地Local域名系统服务器无法解析" + domain);
			return "未部署";
		}
	}

	private static boolean isIPv4(String address)
	{
		return Pattern.compile("^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$").matcher(address).matches();
	}
}
