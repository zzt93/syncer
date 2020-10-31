package com.github.zzt93.syncer.consumer.ack;

import com.google.common.base.Preconditions;
import lombok.Data;

import java.nio.charset.StandardCharsets;

@Data
public class AckMetaData {

	private static final AckMetaData EMPTY = new AckMetaData(new byte[0]);
	private final byte[] bytes;

	AckMetaData(byte[] bytes) {
		Preconditions.checkArgument(bytes != null, "Invalid bytes");
		this.bytes = bytes;
	}

	public static AckMetaData empty() {
		return EMPTY;
	}

	public boolean isEmpty() {
		return bytes.length == 0;
	}

	String toDataStr() {
		return new String(bytes, StandardCharsets.UTF_8);
	}
}
