package com.github.zzt93.syncer.consumer.ack;

import java.io.IOException;

/**
 * @author zzt
 */
public interface MetaFile {

	boolean isExists();

	void createFileAndInitFile();

	AckMetaData readData() throws IOException;

	void putBytes(byte[] bytes) throws IOException;

}
