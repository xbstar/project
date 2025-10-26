package com.xbstar.chenguang;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class BlackHoleSink<T> extends RichSinkFunction<T>
{
	public static Boolean flag = true;
	private boolean first = true;
	private ListState<String> listState;

	@Override
	public void invoke(T value, Context context) throws Exception
	{
		// 直接做丢弃处理
	}
}
