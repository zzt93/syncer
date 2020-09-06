package com.github.zzt93.syncer.consumer.ack;

import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

@Slf4j
public class LocalMetaFileTest {
	private static final Path PATH = Paths.get("src/test/resources/LocalMetaFileTest");
	private LocalMetaFile localMetaFile;

	@Before
	public void setUp() throws Exception {
		localMetaFile = new LocalMetaFile(PATH);
		localMetaFile.createFileAndInitFile();
	}

	@Test
	public void multiThreadTest() throws IOException, InterruptedException {
		ExecutorService two = Executors.newFixedThreadPool(2);
		int times = 10;
		byte[] _003 = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz".getBytes();
		byte[] _117 = "01234567890123456789012345012345678901234567890123450123456789012345678901234501234567890123456789012345".getBytes();
		CountDownLatch start = new CountDownLatch(1);
		CountDownLatch end = new CountDownLatch(2);

		final boolean[] res = {true};
		two.submit(() -> {
			try {
				start.await();
			} catch (InterruptedException ignored) {
			}
			for (int i = 0; i < times; i++) {
				localMetaFile.putBytes(_003);

				byte[] s = new byte[0];
				try {
					s = localMetaFile.readData().getBytes();
				} catch (IOException ignored) {
				}
				res[0] = res[0] && (Arrays.equals(s, _003) || Arrays.equals(s, _117));
				if (!res[0]) {
					log.error("{}", new String(s));
					break;
				}
			}

			end.countDown();
		});
		two.submit(() -> {
			start.countDown();
			for (int i = 0; i < times; i++) {
				localMetaFile.putBytes(_117);

				byte[] s = new byte[0];
				try {
					s = localMetaFile.readData().getBytes();
				} catch (IOException ignored) {
				}
				res[0] = res[0] && (Arrays.equals(s, _003) || Arrays.equals(s, _117));
				if (!res[0]) {
					log.error("{}", new String(s));
					break;
				}
			}
			end.countDown();
		});

		end.await();

		assertTrue(res[0]);
	}

}