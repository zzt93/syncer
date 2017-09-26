package com.github.zzt93.syncer.output;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.output.channel.OutputChannel;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class OutputJob implements Callable<Void> {

  private final BlockingQueue<SyncData> queue;
  private final List<OutputChannel> channels;
  private final Logger logger = LoggerFactory.getLogger(OutputJob.class);

  public OutputJob(BlockingQueue<SyncData> fromFilter, List<OutputChannel> output) {
    this.queue = fromFilter;
    this.channels = output;
  }

  @Override
  public Void call() throws InterruptedException {
    while (!Thread.interrupted()) {
      try {
        SyncData poll = queue.take();
        for (OutputChannel channel : channels) {
          if (!channel.output(poll)) {
            logger.warn("Fail to write to channel {}", channel);
          }
        }
      } catch (Exception e) {
        logger.debug("Output job failed with exception", e);
      }
    }
    return null;
  }
}
