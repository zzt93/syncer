package com.github.zzt93.syncer.consumer.ack;

import com.github.zzt93.syncer.common.thread.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zzt
 * <p>
 * The class use {@link Object#toString()} to get content to write, so you may need to customize
 * this method.
 * @see Object#toString()
 */
@ThreadSafe
public class FileBasedMap<T extends Comparable<T>> {

  private static final AtomicInteger ZERO = new AtomicInteger();
  private final LocalMetaFile localMetaFile;
  private volatile T lastRemoved;
  private final ConcurrentSkipListMap<T, AtomicInteger> map = new ConcurrentSkipListMap<>();
  private final Logger logger = LoggerFactory.getLogger(FileBasedMap.class);

  public FileBasedMap(Path path) {
    localMetaFile = new LocalMetaFile(path);
    localMetaFile.createFileAndInitFile();
  }

  /**
   * @return true if this data is added for first time
   */
  public boolean append(T data, int count) {
    return map.put(data, new AtomicInteger(count)) == null;
  }

  /**
   * @return true if this data is removed from map
   */
  public boolean remove(T data, int count) {
    if (map.getOrDefault(data, ZERO).get() < count) {
      throw new IllegalStateException();
    }
    boolean succ = map.computeIfPresent(data, (k, v) -> v.updateAndGet(c -> c - count) == 0 ? null : v) == null;
    if (succ) {
      lastRemoved = data;
    }
    return succ;
  }

  public boolean flush() {
    T toFlush = getToFlush();
    if (toFlush == null) {
      return false;
    }
    byte[] bytes = toFlush.toString().getBytes(StandardCharsets.UTF_8);
    localMetaFile.putBytes(bytes);
    return true;
  }

  private T getToFlush() {
    if (map.isEmpty()) {
      return lastRemoved;
    }
    // possible race condition isEmpty & remove, firstKey may throw NoSuchElementException
    T first;
    try {
      first = map.firstKey();
    } catch (NoSuchElementException e) {
      return lastRemoved;
    }
    return first;
  }

  @Override
  public String toString() {
    return "FileBasedMap{" +
        "path=" + localMetaFile +
        ", lastRemoved=" + lastRemoved +
        ", map=" + map +
        '}';
  }

  AckMetaData readData() throws IOException {
    return localMetaFile.readData();
  }
}
