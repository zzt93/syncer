package com.github.zzt93.syncer.stat;

import lombok.Getter;
import lombok.ToString;

import java.util.UUID;

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
	 * {@link UUID} version 1 or version 4 (https://www.ietf.org/rfc/rfc4122.txt)
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
	 * <a href="https://github.com/elastic/elasticsearch/blob/fc8c6dd56d602b4a62ee1ff484f00caab92dc6e2/server/src/main/java/org/elasticsearch/env/NodeEnvironment.java#L527>ES node id generation</a>
	 * <a href="https://github.com/elastic/elasticsearch/blob/master/server/src/main/java/org/elasticsearch/common/RandomBasedUUIDGenerator.java">ES impl</a>
	 * @return syncer instanceId
	 */
	private static String generateInstanceId() {
		return UUID.randomUUID().toString();
	}

}
