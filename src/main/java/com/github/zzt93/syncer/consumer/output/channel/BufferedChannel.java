package com.github.zzt93.syncer.consumer.output.channel;

import com.github.zzt93.syncer.common.thread.ThreadSafe;
import java.util.concurrent.TimeUnit;

/**
 * Created by zzt on 9/24/17.
 *
 * <h3></h3>
 */
public interface BufferedChannel<T> extends OutputChannel, AckChannel<T> {

  long getDelay();

  TimeUnit getDelayUnit();

  @ThreadSafe
  void flush() throws InterruptedException;

  @ThreadSafe
  void flushIfReachSizeLimit() throws InterruptedException;

}
