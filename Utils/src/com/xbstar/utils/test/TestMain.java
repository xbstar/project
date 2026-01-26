package com.xbstar.utils.test;

import io.minio.*;
import io.minio.errors.ErrorResponseException;

import java.io.FileInputStream;

public class TestMain
{
	public static String bucket = "minio-janitorial";
	public static String object = "test.js";

	public static void main(String[] args) throws Exception
	{
		MinioClient client = client();
		rename(client);
		client.close();
	}

	public static void upload(MinioClient client) throws Exception
	{
		PutObjectArgs.Builder builder = PutObjectArgs.builder();
		builder.bucket(bucket);
		builder.object(object);
		FileInputStream input = new FileInputStream("mongodb.js");
		builder.stream(input, input.available(), -1);
		ObjectWriteResponse res = client.putObject(builder.build());
		System.out.println(res.bucket());
	}

	public static void rename(MinioClient client) throws Exception
	{
		try
		{
			CopySource source = CopySource.builder().bucket(bucket).object(object).build();
			CopyObjectArgs copyArgs = CopyObjectArgs.builder().bucket(bucket).object("my.js").source(source).build();
			ObjectWriteResponse res = client.copyObject(copyArgs);
		}
		catch (ErrorResponseException e)
		{
			System.out.println(e.errorResponse().code());
		}
	}

	public static MinioClient client()
	{
		MinioClient.Builder builder = MinioClient.builder();
		builder.endpoint("http://10.233.37.215:9000");
		builder.credentials("root", "tobeno1@");
		return builder.build();
	}
}
