package com.github.zzt93.syncer.stat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class SyncerStat {

	private final String instanceId = generateInstanceId();

	private String generateInstanceId() {
		return null;
	}

	private final LongAdder all = new LongAdder();
	private final Map<String, LongAdder> consumers = new HashMap<>();

	public void synced(String consumerId) {
		all.add(1);
		consumers.get(consumerId).add(1);
	}


}
