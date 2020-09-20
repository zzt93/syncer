package com.github.zzt93.syncer.stat;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class SyncerInfo {


	private final String instanceId;
	private final String version;

	public SyncerInfo(String version) {
		this.instanceId = generateInstanceId();
		this.version = version;
	}

	/**
	 * <ul>
	 *   <li>Unique if start in different machine</li>
	 *   <li>Unique if start multiple instances in same machine at different time</li>
	 *   <li>May conflict if start multiple instances in same machine at exactly same time</li>
	 * </ul>
	 * <pre>
	 * InstanceId
	 *   -------------------------
	 *   machineId | time | random
	 *   -------------------------
	 * </pre>
	 * @return syncer instanceId
	 */
	private static String generateInstanceId() {
		return null;
	}

}
