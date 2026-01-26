package com.xbstar.minio;

import com.xbstar.spring.XBStarException;
import com.xbstar.utils.MinIOUtils;
import com.xbstar.utils.PrintUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
public interface MinIOSpring
{
	@RequestMapping("minio/upload-media")
	default String uploadMedia(@RequestParam String bucket, @RequestParam String name, @RequestParam MultipartFile file) throws Exception
	{
		//		System.out.println("原始文件名: " + file.getOriginalFilename());
		//		System.out.println("文件大小: " + file.getSize() + " bytes");
		if (!name.contains("."))//没有文件后缀名自动补全
		{
			String type = file.getContentType();
			switch (type)
			{
				case "image/jpeg":
				{
					name += ".jpg";
					break;
				}
				case "image/png":
				{
					name += ".png";
					break;
				}
				case "video/mp4":
				{
					name += ".mp4";
					break;
				}
				default:
				{
					PrintUtils.printWarning("[MinIO]遇到没有处理的文件类型" + type);
					name += ".obj";
					break;
				}
			}
		}
		try
		{
			return MinIOUtils.uploadMedia(bucket, name, file.getInputStream());
		} catch (MinIOUtils.BucketNotExistException e)
		{
			throw new XBStarException(400, e.getMessage());
		}
	}

	@RequestMapping("minio/rename-media")
	default String renameMedia(String bucket, String old, String name) throws Exception
	{
		try
		{
			return MinIOUtils.renameMedia(bucket, old, name);
		} catch (MinIOUtils.BucketNotExistException | MinIOUtils.MediaNotExistException e)
		{
			throw new XBStarException(400, e.getMessage());
		}
	}

	@RequestMapping("minio/delete-media")
	default String deleteMedia(@RequestParam String bucket, @RequestParam String name) throws Exception
	{
		try
		{
			return MinIOUtils.deleteMedia(bucket, name);
		} catch (MinIOUtils.BucketNotExistException | MinIOUtils.MediaNotExistException e)
		{
			throw new XBStarException(400, e.getMessage());
		}
	}
}
