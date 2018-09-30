package com.github.zzt93.syncer.consumer.output.channel;

import com.github.zzt93.syncer.common.thread.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by zzt on 9/24/17.
 *
 * <h3></h3>
 */
public interface BufferedChannel<T> extends OutputChannel, AckChannel<T> {
  Logger logger = LoggerFactory.getLogger(BufferedChannel.class);

  long getDelay();

  TimeUnit getDelayUnit();

  @ThreadSafe
  void flush() throws InterruptedException;

  @ThreadSafe
  void flushIfReachSizeLimit() throws InterruptedException;

  @Override
  default void close() {
    try {
      flush();
      // waiting for response from remote to clear ack
      while (checkpoint()) {
        logger.info("Waiting for clear ack info ...");
        Thread.sleep(1000);
      }
    } catch (InterruptedException e) {
      logger.warn("Interrupt {}#close", getClass().getSimpleName());
    }
  }
}
