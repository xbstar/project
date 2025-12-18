(function ()
{
	let c = db.serverStatus().wiredTiger.cache;
	let current = c["bytes currently in the cache"] / 1000 / 1000 / 1000;
	let total = c["maximum bytes configured"] / 1000 / 1000 / 1000;
	return JSON.stringify({
		"Cache使用率": `${current.toFixed(5)} GB / ${total.toFixed(2)} GB = ${(current/total*100).toFixed(2)} %`,
		"总内存中页数": c["pages currently held in the cache"],
		"更新脏页页数": c["tracked dirty pages in the cache"],
		"强制驱逐次数": c["forced eviction - pages evicted that were dirty count"],
		"驱逐队列长度": c["pages queued for eviction"],
		"驱逐扫描次数": c["pages walked for eviction"],
		"扫描失败次数": c["eviction walks gave up because they saw too many pages and found no candidates"],
		"运行的检查点": db.currentOp().inprog.filter(op => op.op === "command" && op.command.checkpoint),
		"磁盘累计写入": c["pages written from cache"]
	}, null, 2);
})()
