package com.github.zzt93.syncer.consumer.output.batch;

import com.github.zzt93.syncer.common.exception.ShutDownException;
import com.github.zzt93.syncer.common.thread.EventLoop;
import com.github.zzt93.syncer.consumer.output.channel.BufferedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class BatchJob implements EventLoop {

  private static final Logger logger = LoggerFactory.getLogger(BatchJob.class);


  private final BufferedChannel bufferedChannel;

  public BatchJob(BufferedChannel bufferedChannel) {
    this.bufferedChannel = bufferedChannel;
  }

  @Override
  public void loop() {
    try {
      bufferedChannel.flush();
    } catch (InterruptedException e) {
      logger.warn("Batch job interrupted");
      throw new ShutDownException(e);
    }
  }
}
