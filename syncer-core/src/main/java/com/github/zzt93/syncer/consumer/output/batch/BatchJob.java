package com.github.zzt93.syncer.consumer.output.batch;

import com.github.zzt93.syncer.common.exception.FailureException;
import com.github.zzt93.syncer.common.exception.ShutDownException;
import com.github.zzt93.syncer.common.thread.EventLoop;
import com.github.zzt93.syncer.consumer.output.channel.BufferedChannel;
import com.github.zzt93.syncer.health.Health;
import com.github.zzt93.syncer.health.SyncerHealth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class BatchJob implements EventLoop {

  private static final Logger logger = LoggerFactory.getLogger(BatchJob.class);


  private final BufferedChannel bufferedChannel;
  private final String consumerId;

  public BatchJob(String consumerId, BufferedChannel bufferedChannel) {
    this.bufferedChannel = bufferedChannel;
    this.consumerId = consumerId;
  }

  @Override
  public void loop() {
    try {
      logger.debug("Flushing by batch job");
      bufferedChannel.flushAndSetFlushDone(false);
    } catch (InterruptedException e) {
      logger.warn("Batch job interrupted");
      throw new ShutDownException(e);
    } catch (FailureException e) {
      String err = FailureException.getErr(bufferedChannel, consumerId);
      logger.error(err, e);
      SyncerHealth.consumer(consumerId, bufferedChannel.id(), Health.red(err));
    }
  }
}
