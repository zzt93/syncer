package com.github.zzt93.syncer.consumer.ack;

import lombok.Data;

import java.nio.charset.StandardCharsets;

@Data
public class AckMetaData {

	private final byte[] bytes;

	public AckMetaData(byte[] bytes) {
		this.bytes = bytes;
	}


	public boolean isEmpty() {
		return bytes.length == 0;
	}

	String toDataStr() {
		return new String(bytes, StandardCharsets.UTF_8);
	}
}
