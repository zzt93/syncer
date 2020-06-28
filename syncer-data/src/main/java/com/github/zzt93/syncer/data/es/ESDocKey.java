package com.github.zzt93.syncer.data.es;

import lombok.Data;
import lombok.RequiredArgsConstructor;

/**
 * @author zzt
 */
@RequiredArgsConstructor
@Data
public class ESDocKey {
	private final String name;

	public static ESDocKey of(String name) {
		return new ESDocKey(name);
	}
}
