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
import java.util.concurrent.ConcurrentSkipListSet;
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
public class FileBasedSet<T extends Comparable<T>> {

  private static final int _1K = 1024;
  private final MappedByteBuffer file;
  private final ConcurrentSkipListSet<T> set = new ConcurrentSkipListSet<>();
  private final Logger logger = LoggerFactory.getLogger(FileBasedSet.class);

  public FileBasedSet(Path path) throws IOException {
    Files.createDirectories(path.getParent());
    try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(path, EnumSet
        .of(StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE))) {
      file = fileChannel.map(MapMode.READ_WRITE, 0, _1K);
    }
  }

  public boolean append(T data) {
    return set.add(data);
  }

  public boolean remove(T data) {
    return set.remove(data);
  }

  public void flush() {
    if (set.isEmpty()) {
      return;
    }
    T first = set.first();
    logger.debug("Flushing ack info {}", first);
    file.clear();
    try {
      file.put(first.toString().getBytes("utf-8"));
    } catch (UnsupportedEncodingException ignore) {
      logger.error("Impossible", ignore);
    }
    file.force();
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

}
