package com.xbstar.utils;

import io.minio.*;
import io.minio.errors.ErrorResponseException;
import org.springframework.web.context.ContextLoader;

import java.io.InputStream;

public class MinIOUtils
{
	public static String MinIOServerAddress = EnvUtils.getConfigFromEnvironment(String.class, "minio.server");
	public static String MinIOUsername = EnvUtils.getConfigFromEnvironment(String.class, "minio.username");
	public static String MinIOPassword = EnvUtils.getConfigFromEnvironment(String.class, "minio.password");
	public static MinioClient MinIOClient = null;

	static
	{
		if (MinIOServerAddress == null) throw new RuntimeException("[MinIO]没有配置minio.server");
		if (MinIOUsername == null) throw new RuntimeException("[MinIO]没有配置minio.username");
		if (MinIOPassword == null) throw new RuntimeException("[MinIO]没有配置minio.password");
		PrintUtils.printInfo("[MinIO]读取到配置minio.server=" + MinIOServerAddress);
		PrintUtils.printInfo("[MinIO]读取到配置minio.username=" + MinIOUsername);
		PrintUtils.printInfo("[MinIO]读取到配置minio.password=" + MinIOPassword);
		MinioClient.Builder builder = MinioClient.builder();
		builder.endpoint(MinIOServerAddress);
		builder.credentials(MinIOUsername, MinIOPassword);
		MinIOClient = builder.build();
	}

	public static class BucketNotExistException extends RuntimeException
	{
		public BucketNotExistException(String message) {super(message);}
	}

	public static class MediaNotExistException extends RuntimeException
	{
		public MediaNotExistException(String message) {super(message);}
	}

	public static String getBucketFromURL(String url)
	{
		url = url.replace("//", "");
		for (String cur : url.split("/"))
		{
			if (cur.toUpperCase().startsWith("HTTP") || cur.toUpperCase().startsWith("HTTPS") || "".equals(cur.trim())) continue;
			return cur;
		}
		throw new BucketNotExistException(url + "中没有找到桶信息");
	}

	public static String getObjectFromURL(String url)
	{
		String bucket = getBucketFromURL(url);
		String path = url.substring(url.indexOf(bucket) + bucket.length() + 1);
		if (path.trim().endsWith("/")) path = path.substring(0, path.lastIndexOf("/"));
		String object = path.contains("/") ? path.substring(path.lastIndexOf("/") + 1) : path;
		if ("".equals(object)) throw new BucketNotExistException(url + "中没有找到桶信息");
		return object;
	}

	public static String getFileName(String url)
	{
		String object = getObjectFromURL(url);
		return object.contains(".") ? object.substring(0, object.lastIndexOf(".")) : object;
	}

	public static String getFileType(String url)
	{
		String object = getObjectFromURL(url);
		return object.contains(".") ? object.substring(object.lastIndexOf(".") + 1) : "obj";
	}

	public static String uploadMedia(String bucket, String name, InputStream input)
	{
		try
		{
			// 检查目标桶是否存在
			if (!MinIOClient.bucketExists(BucketExistsArgs.builder().bucket(bucket).build())) throw new BucketNotExistException("桶" + bucket + "不存在");
			// 构建上传参数
			PutObjectArgs.Builder builder = PutObjectArgs.builder();
			builder.bucket(bucket);
			builder.object(name);
			String type = name.contains(".") ? name.substring(name.lastIndexOf(".") + 1) : "obj";
			switch (type) //设置MinIO请求的媒体返回类型
			{
				case "pdf":
				{
					builder.contentType("application/pdf");
					break;
				}
				case "jpg":
				{
					builder.contentType("image/jpeg");
					break;
				}
				case "png":
				{
					builder.contentType("image/png");
					break;
				}
				case "gif":
				{
					builder.contentType("image/gif");
					break;
				}
				case "ico":
				{
					builder.contentType("image/x-icon");
					break;
				}
				case "svg":
				{
					builder.contentType("image/svg+xml");
					break;
				}
				case "obj":
				{
					PrintUtils.printWarning("[MinIO]没有从文件名" + name + "中获取到类型");
					break;
				}
				default:
				{
					PrintUtils.printWarning("[MinIO]遗漏的文件类型" + type);
					break;
				}
			}
			// 获取媒体文件输入流
			builder.stream(input, input.available(), -1);
			// 执行文件上传
			MinIOClient.putObject(builder.build());
			input.close(); //关闭输入流
			return "/" + bucket + "/" + name;
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public static String renameMedia(String bucket, String old, String name)
	{
		if (!old.equals(name))
		{
			try
			{
				// 检查目标桶是否存在
				if (!MinIOClient.bucketExists(BucketExistsArgs.builder().bucket(bucket).build())) throw new BucketNotExistException("桶" + bucket + "不存在");
				// 首先拷贝目标到新的名称
				CopySource source = CopySource.builder().bucket(bucket).object(old).build();
				CopyObjectArgs copyArgs = CopyObjectArgs.builder().bucket(bucket).object(name).source(source).build();
				MinIOClient.copyObject(copyArgs);
				// 然后删除源文件
				RemoveObjectArgs removeArgs = RemoveObjectArgs.builder().bucket(bucket).object(old).build();
				MinIOClient.removeObject(removeArgs);

			} catch (ErrorResponseException e)
			{
				if ("NoSuchKey".equals(e.errorResponse().code())) throw new MediaNotExistException("桶" + bucket + "中不存在" + old);
				else throw new RuntimeException(e);
			} catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}
		return "/" + bucket + "/" + name;
	}

	public static String deleteMedia(String bucket, String name)
	{
		try
		{
			MinioClient client = ContextLoader.getCurrentWebApplicationContext().getBean(MinioClient.class);
			RemoveObjectArgs removeArgs = RemoveObjectArgs.builder().bucket(bucket).object(name).build();
			client.removeObject(removeArgs);
		} catch (ErrorResponseException e)
		{
			if (!"NoSuchKey".equals(e.errorResponse().code())) throw new RuntimeException(e);
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
		return "/" + bucket + "/" + name;
	}
}
