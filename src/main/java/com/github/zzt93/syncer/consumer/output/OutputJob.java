package com.github.zzt93.syncer.consumer.output;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.event.RowsEvent;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * @author zzt
 */
public class OutputJob implements Callable<Void> {

  private final BlockingDeque<SyncData> queue;
  private final List<OutputChannel> channels;
  private final Logger logger = LoggerFactory.getLogger(OutputJob.class);

  public OutputJob(BlockingDeque<SyncData> fromFilter, List<OutputChannel> output) {
    this.queue = fromFilter;
    this.channels = output;
  }

  @Override
  public Void call() throws InterruptedException {
    while (!Thread.interrupted()) {
      SyncData poll = null;
      try {
        poll = queue.take();
        MDC.put(RowsEvent.EID, poll.getEventId());
        for (OutputChannel channel : channels) {
          if (!channel.output(poll)) {
            logger.warn("Fail to write to channel {}", channel);
          }
        }
      } catch (Exception e) {
        logger.error("Output job failed with exception", e);
        queue.addFirst(poll);
      }
    }
    return null;
  }
}
