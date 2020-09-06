package com.github.zzt93.syncer.consumer.ack;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;

@Slf4j
public class LocalAndEtcdMetaFile implements MetaFile {

	private final LocalMetaFile localMetaFile;
	private final EtcdBasedFile etcdBasedFile;
	private long lastUpdateTime;
	private int lastHash;
	private static final long _10s = 10 * 1000;


	public LocalAndEtcdMetaFile(LocalMetaFile localMetaFile, EtcdBasedFile etcdBasedFile) {
		this.localMetaFile = localMetaFile;
		this.etcdBasedFile = etcdBasedFile;
	}

	@Override
	public boolean isExists() {
		return localMetaFile.isExists();
	}

	@Override
	public void createFileAndInitFile() {
		localMetaFile.createFileAndInitFile();
		etcdBasedFile.createFileAndInitFile();
	}


	@Override
	public AckMetaData readData() throws IOException {
		AckMetaData bytes = localMetaFile.readData();
		if (bytes.isEmpty()) {
			bytes = etcdBasedFile.readData();
			localMetaFile.putBytes(bytes.getBytes());
			return bytes;
		}
		return bytes;
	}

	@Override
	public void putBytes(byte[] bytes) throws IOException {
		localMetaFile.putBytes(bytes);
		if (System.currentTimeMillis() - lastUpdateTime > _10s) {
			int thisHash = Arrays.hashCode(bytes);
			if (lastHash != thisHash) {
				etcdBasedFile.putBytes(bytes);
				lastHash = thisHash;
				lastUpdateTime = System.currentTimeMillis();
			} else {
				log.debug("{}, {}, {}", lastUpdateTime, lastHash, log.isDebugEnabled() ? new String(bytes) : null);
			}
		}
	}
}
