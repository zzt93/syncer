package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.common.thread.ThreadSafe;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
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

  private static final int _1M = 1048576;
  private final MappedByteBuffer file;
  private final ConcurrentSkipListSet<T> set = new ConcurrentSkipListSet<>();
  private final Logger logger = LoggerFactory.getLogger(FileBasedSet.class);

  public FileBasedSet(Path path) throws IOException {
    try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(path, EnumSet
        .of(StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING))) {
      file = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, _1M);
    }
  }

  public boolean append(T data) {
    return set.add(data);
  }

  public boolean remove(T data) {
    return set.remove(data);
  }

  public void flush() {
    file.clear();
    for (T t : set) {
      try {
        file.put(t.toString().getBytes("utf-8"));
      } catch (UnsupportedEncodingException ignore) {
        logger.error("Impossible", ignore);
      }
    }
    file.force();
  }

}
