package com.github.zzt93.syncer.consumer.ack;

import com.github.zzt93.syncer.common.thread.WaitingAckHook;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.consumer.output.channel.AckChannel;
import com.google.common.primitives.Bytes;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.LinkedList;

/**
 * Preconditions:
 *  ----------------------------------------------
 * |###/.../###0000
 *  ----------------------------------------------
 *             |
 *          position
 * @author zzt
 */
@Slf4j
public class LocalMetaFile implements MetaFile {

	private static final int _1K = 1024;
	private static final byte DEFAULT = 0;
	private final Path path;
	private MappedByteBuffer file;

	public LocalMetaFile(Path path) {
		this.path = path;
	}

	/**
	 * init memory mapped file's position, otherwise will affect putBytes
	 * @see #putBytes(byte[])
	 */
	private void initFile() {
		int i;
		for (i = 0; i < _1K && /*get and inc position*/file.get() != DEFAULT; i++) ;
	}

	@Override
	public void createFileAndInitFile() {
		if (!isExists()) {
			log.info("Last run meta file[{}] not exists, fresh run", path);
		}
		try {
			// create file
			Files.createDirectories(path.toAbsolutePath().getParent());
			try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(path, EnumSet
					.of(StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE))) {
				file = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, _1K);
			}
		} catch (IOException e) {
			log.error("Fail to create file {}", path);
			throw new InvalidConfigException("Fail to create file: " + path);
		}
		initFile();
	}

	@Override
	public boolean isExists() {
		return Files.exists(this.path);
	}

	@Override
	public synchronized AckMetaData readData() throws IOException {
		LinkedList<Byte> bytes = new LinkedList<>();
		for (int i = 0; i < _1K && /*get but not inc position*/file.get(i) != DEFAULT; i++) {
			bytes.add(file.get(i));
		}
		return new AckMetaData(Bytes.toArray(bytes));
	}


	/**
	 * In most cases only invoke by PositionFlusher. When shutdown,
	 * may invoke by shutdown hook thread
	 * @see PositionFlusher
	 * @see AckChannel#checkpoint()
	 * @see WaitingAckHook
	 */
	@Override
	public synchronized void putBytes(byte[] bytes) {
		for (int i = 0; i < bytes.length; i++) {
			file.put(i, bytes[i]);
		}
		int position = file.position();
		for (int i = bytes.length; i < position; i++) {
			file.put(i, DEFAULT);
		}
		file.position(bytes.length);

		file.force();
	}

	@Override
	public String toString() {
		return "LocalMetaFile{" +
				"path=" + path +
				'}';
	}
}
