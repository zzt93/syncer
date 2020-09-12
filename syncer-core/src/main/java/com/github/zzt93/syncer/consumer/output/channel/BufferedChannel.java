package com.github.zzt93.syncer.consumer.output.channel;

import com.github.zzt93.syncer.ShutDownCenter;
import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.github.zzt93.syncer.stat.vo.BatchBufferStat;
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

  default BatchBufferStat getBufferStatistic() {
    throw new UnsupportedOperationException();
  }

  @ThreadSafe
  boolean flush() throws InterruptedException;

  @ThreadSafe
  boolean flushIfReachSizeLimit() throws InterruptedException;

  @ThreadSafe
  default void flushAndSetFlushDone(boolean bySize) throws InterruptedException {
    boolean res;
    if (bySize) {
      res = flushIfReachSizeLimit();
    } else {
      res = flush();
    }
    if (res) {
      setFlushDone();
    }
  }

  void setFlushDone();

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
