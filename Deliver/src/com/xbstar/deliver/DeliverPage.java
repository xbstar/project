package com.xbstar.deliver;

import java.util.List;

public class DeliverPage<T extends Enum<?>>
{
	public long count;
	public List<Deliver<T>> data;

	DeliverPage() {} //只有同包可以调用构造函数

	@Override
	public String toString()
	{
		return "本页" + data.size() + "条,总计" + count + "条";
	}
}
