package com.github.zzt93.syncer.data.es;

import lombok.Data;
import lombok.RequiredArgsConstructor;

/**
 * @author zzt
 */
@RequiredArgsConstructor
@Data
public class SyncDataKey {
	private final String name;

	public static SyncDataKey of(String name) {
		return new SyncDataKey(name);
	}
}
