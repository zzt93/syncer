package com.github.zzt93.syncer.consumer.ack;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;

@Slf4j
public class LocalAndEtcdMetaFile implements MetaFile {

	private final LocalMetaFile localMetaFile;
	private final EtcdBasedFile etcdBasedFile;
	private long lastUpdateTime;
	private byte[] lastWrite;
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
		if (!bytes.isEmpty()) { // first check local
			return bytes;
		}
		// fallback to remote
		bytes = etcdBasedFile.readData();
		if (bytes.isEmpty()) { // remote is also empty
			return bytes;
		}
		localMetaFile.putBytes(bytes.getBytes());
		return bytes;
	}

	@Override
	public void putBytes(byte[] bytes) throws IOException {
		localMetaFile.putBytes(bytes);
		syncToRemote(bytes);
	}

	private void syncToRemote(byte[] bytes) throws IOException {
		if (System.currentTimeMillis() - lastUpdateTime > _10s) {
			if (Arrays.equals(lastWrite, bytes)) {
				etcdBasedFile.putBytes(bytes);
				lastWrite = bytes;
				lastUpdateTime = System.currentTimeMillis();
			} else {
				log.debug("{}, {}, {}", lastUpdateTime, lastWrite, log.isDebugEnabled() ? new String(bytes) : null);
			}
		}
	}
}
