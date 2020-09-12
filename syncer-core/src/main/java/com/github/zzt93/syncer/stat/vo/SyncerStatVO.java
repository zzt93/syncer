package com.github.zzt93.syncer.stat.vo;

public class SyncerStatVO {

	private final String instanceId;
	private final long synced;

	public SyncerStatVO(String instanceId, long synced) {
		this.instanceId = instanceId;
		this.synced = synced;
	}
}
