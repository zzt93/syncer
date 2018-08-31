package com.github.zzt93.syncer.consumer.ack;

import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.google.common.primitives.Bytes;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 *
 * The class use {@link Object#toString()} to get content to write, so you may need to customize
 * this method.
 * @see Object#toString()
 */
@ThreadSafe
public class FileBasedMap<T extends Comparable<T>> {

  private static final int _1K = 1024;
  private final MappedByteBuffer file;
  private final ConcurrentSkipListMap<T, AtomicInteger> map = new ConcurrentSkipListMap<>();
  private final Logger logger = LoggerFactory.getLogger(FileBasedMap.class);
  private final int maxKeyLen;

  public FileBasedMap(Path path, int maxKeyLen) throws IOException {
    this.maxKeyLen = maxKeyLen;
    Files.createDirectories(path.toAbsolutePath().getParent());
    try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(path, EnumSet
        .of(StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE))) {
      file = fileChannel.map(MapMode.READ_WRITE, 0, _1K);
    }
  }

  public static byte[] readData(Path path) throws IOException {
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

  public AtomicInteger append(T data, int count) {
    return map.put(data, new AtomicInteger(count));
  }

  public AtomicInteger remove(T data, int count) {
    if (map.getOrDefault(data, new AtomicInteger()).get() < count) {
      throw new IllegalStateException();
    }
    return map.computeIfPresent(data, (k, v) -> v.updateAndGet(x -> x - count) == 0 ? null : v);
  }

  public void flush() {
    if (map.isEmpty()) {
      return;
    }
    T first = map.firstKey();
//    logger.debug("Flushing ack info {}", first);
    clearBuf(maxKeyLen);
    try {
      byte[] bytes = first.toString().getBytes("utf-8");
      if (bytes.length > maxKeyLen) {
        // TODO 18/8/31
        logger.error("key too long {}", first);
      }
      file.put(bytes);
    } catch (UnsupportedEncodingException ignore) {
      logger.error("Impossible", ignore);
    }
    file.force();
  }

  private void clearBuf(int size) {
    for (int i = 0; i < size; i++) {
      file.put(i, (byte) 0);
    }
    file.position(0);
  }

}
