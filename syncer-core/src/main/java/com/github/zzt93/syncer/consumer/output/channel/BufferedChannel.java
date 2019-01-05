package com.github.zzt93.syncer.consumer.output.channel;

import com.github.zzt93.syncer.ShutDownCenter;
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
      int i = 0;
      while (i++ < ShutDownCenter.SHUTDOWN_MAX_TRY && checkpoint()) {
        logger.info("[Shutting down] Waiting {} clear ack info ...", getClass().getSimpleName());
        Thread.sleep(1000);
      }
    } catch (InterruptedException e) {
      logger.warn("[Shutting down] Interrupt {}#close", getClass().getSimpleName());
    }
  }
}
