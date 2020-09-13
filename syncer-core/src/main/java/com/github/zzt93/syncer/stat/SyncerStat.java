package com.github.zzt93.syncer.stat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class SyncerStat {

	private static SyncerStat syncerStat;
	private final SyncerInfo syncerInfo;
	private final LongAdder all = new LongAdder();
	private final Map<String, LongAdder> consumers = new HashMap<>();

	private SyncerStat(SyncerInfo syncerInfo, List<String> consumerIds) {
		this.syncerInfo = syncerInfo;
		for (String consumerId : consumerIds) {
			consumers.put(consumerId, new LongAdder());
		}
	}

	public static synchronized void init(SyncerInfo syncerInfo, List<String> consumerIds) {
		syncerStat = new SyncerStat(syncerInfo, consumerIds);
	}

	public static void synced(String consumerId) {
		syncerStat.all.add(1);
		syncerStat.consumers.get(consumerId).add(1);
	}

	public static void synced(String consumerId, long size) {
		syncerStat.all.add(size);
		syncerStat.consumers.get(consumerId).add(size);
	}


}
