package com.github.zzt93.syncer.consumer.ack;

import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.google.common.primitives.Bytes;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.LinkedList;

/**
 * @author zzt
 */
@Slf4j
public class LocalMetaFile implements MetaFile {

	private static final int _1K = 1024;
	private final Path path;
	private MappedByteBuffer file;

	public LocalMetaFile(Path path) {
		this.path = path;
	}

	@Override
	public void initFile() {
		// init memory mapped file
		int i;
		for (i = 0; i < _1K && file.get(i) != 0; i++) ;
		file.position(i);
	}

	@Override
	public void createFile() {
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
	}

	@Override
	public boolean isExists() {
		return Files.exists(this.path);
	}

	@Override
	public byte[] readData() throws IOException {
		LinkedList<Integer> bytes = new LinkedList<>();
		try (BufferedReader br = new BufferedReader(
				new InputStreamReader(Files.newInputStream(path)))) {
			int ch;
			while ((ch = br.read()) != -1) {
				if (ch == 0) {
					break;
				} else {
					bytes.add(ch);
				}
			}
		}
		return Bytes.toArray(bytes);
	}


	@Override
	public void putBytes(byte[] bytes) {
		for (int i = 0; i < bytes.length; i++) {
			file.put(i, bytes[i]);
		}
		int position = file.position();
		for (int i = bytes.length; i < position; i++) {
			file.put(i, (byte) 0);
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
