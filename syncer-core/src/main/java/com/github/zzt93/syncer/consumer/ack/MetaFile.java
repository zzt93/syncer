package com.github.zzt93.syncer.consumer.ack;

import com.github.zzt93.syncer.common.thread.WaitingAckHook;
import com.github.zzt93.syncer.consumer.output.channel.AckChannel;

import java.io.IOException;

/**
 * @author zzt
 */
public interface MetaFile {

	boolean isExists();

	void createFileAndInitFile();

	AckMetaData readData() throws IOException;

	/**
	 * In most cases only invoke by PositionFlusher. When shutdown,
	 * may invoke by shutdown hook thread
	 * @see PositionFlusher
	 * @see AckChannel#checkpoint()
	 * @see WaitingAckHook
	 */
	void putBytes(byte[] bytes) throws IOException;

	String toString();

}
