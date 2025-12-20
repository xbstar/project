package com.xbstar.spring;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class XBStarPage
{
	/* 请求来的参数*/
	public int pageIndex;
	public int pageVolume;
	public int recordIndex;
	/* 需要通过接口装填的数据*/
	public int totalCount;
	public List<Map<String, Object>> pageData = Collections.EMPTY_LIST;
	/** 接口状态可以附加一些数据*/
	public Map<String, Object> attachData = new HashMap<>();

	public int getPageIndex()
	{
		return pageIndex;
	}

	public void setPageIndex(int pageIndex)
	{
		this.pageIndex = pageIndex;
	}

	public int getPageVolume()
	{
		return pageVolume;
	}

	public void setPageVolume(int pageVolume)
	{
		this.pageVolume = pageVolume;
	}

	public int getRecordIndex()
	{
		return recordIndex;
	}

	public void setRecordIndex(int recordIndex)
	{
		this.recordIndex = recordIndex;
	}

	public int getTotalCount()
	{
		return totalCount;
	}

	public void setTotalCount(int totalCount)
	{
		this.totalCount = totalCount;
	}

	public List<Map<String, Object>> getPageData()
	{
		return pageData;
	}

	public void setPageData(List<Map<String, Object>> pageData)
	{
		this.pageData = pageData;
	}

	public Map<String, Object> getAttachData()
	{
		return attachData;
	}

	public void setAttachData(Map<String, Object> attachData)
	{
		this.attachData = attachData;
	}
}
