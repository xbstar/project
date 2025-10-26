package com.xbstar.http;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;

public class DnsResolver
{
	public static String resolve(String domain, String dnsServer, int dnsPort)
	{
		byte[] dnsQuery = null;
		try
		{
			// 构造 DNS 查询报文
			dnsQuery = buildDnsQuery(domain);
		} catch (Exception e)
		{
			XBStarUtils.printError("[XBStarHttp]不能构造请求报文:" + e.getMessage());
			return "出错了";
		}
		OutputStream outputStream;
		InputStream inputStream;
		try
		{
			// 使用 TCP 连接到 DNS 服务器
			Socket socket = new Socket(dnsServer, dnsPort);
			outputStream = socket.getOutputStream();
			inputStream = socket.getInputStream();
		} catch (Exception e)
		{
			XBStarUtils.printError("[XBStarHttp]无法连接DNS服务器" + dnsServer + ":" + e.getMessage());
			return "出错了";
		}
		try
		{
			// 发送 DNS 查询报文
			byte[] lengthPrefix = ByteBuffer.allocate(2).putShort((short) dnsQuery.length).array();
			outputStream.write(lengthPrefix); // TCP DNS 查询需要添加长度前缀
			outputStream.write(dnsQuery);
		} catch (Exception e)
		{
			XBStarUtils.printError("[XBStarHttp]无法发送请求报文:" + e.getMessage());
			return "出错了";
		}
		byte[] responseBuffer;
		try
		{
			// 读取响应
			byte[] responseLengthBuffer = new byte[2];
			inputStream.read(responseLengthBuffer); // 先读取长度
			int responseLength = ByteBuffer.wrap(responseLengthBuffer).getShort();
			responseBuffer = new byte[responseLength];
			inputStream.read(responseBuffer); // 读取完整响应
		} catch (Exception e)
		{
			XBStarUtils.printError("[XBStarHttp]读取响应报文出错:" + e.getMessage());
			return "出错了";
		}
		try
		{
			// 解析响应报文
			return parseDnsResponse(responseBuffer, domain);
		} catch (Exception e)
		{
			XBStarUtils.printError("[XBStarHttp]解析响应报文出错:" + e.getMessage());
			return "出错了";
		}
	}

	// 构造 DNS 查询报文
	private static byte[] buildDnsQuery(String domain) throws Exception
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		// 标识符（2字节）随机生成
		baos.write(new byte[] { 0x12, 0x34 });
		// 标志位（2字节）：标准查询
		baos.write(new byte[] { 0x01, 0x00 });
		// 问题数（2字节）
		baos.write(new byte[] { 0x00, 0x01 });
		// 资源记录数（回答、权威回答、附加记录均为 0）
		baos.write(new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 });
		// 构造查询部分
		String[] domainParts = domain.split("\\.");
		for (String part : domainParts)
		{
			baos.write(part.length()); // 每部分长度
			baos.write(part.getBytes()); // 部分内容
		}
		baos.write(0x00); // 结束标志
		// 查询类型（A记录，1）
		baos.write(new byte[] { 0x00, 0x01 });
		// 查询类（IN，1）
		baos.write(new byte[] { 0x00, 0x01 });
		return baos.toByteArray();
	}

	private static String parseDnsResponse(byte[] response, String domain) throws Exception
	{
		ByteBuffer buffer = ByteBuffer.wrap(response);
		// 解析报文头部
		int transactionId = buffer.getShort() & 0xFFFF; // 事务 ID
		int flags = buffer.getShort() & 0xFFFF; // 标志
		int questionCount = buffer.getShort() & 0xFFFF; // 问题数
		int answerCount = buffer.getShort() & 0xFFFF; // 回答数
		int authorityCount = buffer.getShort() & 0xFFFF; // 权威回答数
		int additionalCount = buffer.getShort() & 0xFFFF; // 附加记录数
		// 跳过问题部分
		for (int i = 0; i < questionCount; i++)
		{
			skipDomainName(buffer); // 跳过域名部分
			buffer.position(buffer.position() + 4); // 跳过查询类型和类
		}
		// 解析回答部分
		StringBuilder ipAddresses = new StringBuilder();
		for (int i = 0; i < answerCount; i++)
		{
			skipDomainName(buffer); // 跳过域名部分
			int type = buffer.getShort() & 0xFFFF; // 记录类型
			int clazz = buffer.getShort() & 0xFFFF; // 类
			int ttl = buffer.getInt(); // 生存时间
			int dataLength = buffer.getShort() & 0xFFFF; // 数据长度
			if (type == 1 && clazz == 1 && dataLength == 4)
			{    // IPv4 地址（A记录）
				byte[] addressBytes = new byte[dataLength];
				buffer.get(addressBytes);
				InetAddress address = InetAddress.getByAddress(addressBytes);
				ipAddresses.append(address.getHostAddress()).append("\n");
			}
			else
			{
				buffer.position(buffer.position() + dataLength); // 跳过非 IPv4 记录
			}
		}
		if (ipAddresses.length() <= 0)
		{
			if (XBStarHttp.OnErrorPrint) XBStarUtils.printWarning("[XBStarHttp]集群Cluster域名系统服务器无法解析" + domain);
			return "未部署";
		}
		return ipAddresses.toString().trim();
	}

	// 跳过压缩或普通域名
	private static void skipDomainName(ByteBuffer buffer)
	{
		while (true)
		{
			byte length = buffer.get();
			if ((length & 0xC0) == 0xC0)
			{
				// 压缩域名：高两位为 11 表示跳转
				buffer.get(); // 跳过压缩指针
				break;
			}
			else if (length == 0)
			{
				// 普通域名结束
				break;
			}
			else
			{
				// 普通域名部分
				buffer.position(buffer.position() + length);
			}
		}
	}
}
