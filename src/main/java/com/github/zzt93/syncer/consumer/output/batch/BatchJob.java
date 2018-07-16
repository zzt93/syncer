package com.github.zzt93.syncer.consumer.output.batch;

import com.github.zzt93.syncer.consumer.output.channel.BufferedChannel;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class BatchJob implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(BatchJob.class);


  private final BufferedChannel bufferedChannel;

  public BatchJob(BufferedChannel bufferedChannel) {
    this.bufferedChannel = bufferedChannel;
  }

  @Override
  public void run() {
    try {
      bufferedChannel.flush();
    } catch (Throwable e) {
      logger.error("Batch job failed with", e);
      Throwables.throwIfUnchecked(e);
    }
  }
}
