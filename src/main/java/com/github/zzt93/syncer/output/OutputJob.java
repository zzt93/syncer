package com.github.zzt93.syncer.output;

import com.github.zzt93.syncer.common.SyncData;
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
  public Void call() {
    while (!Thread.interrupted()) {
      SyncData poll = queue.poll();
      for (OutputChannel channel : channels) {
        if (!channel.output(poll)) {
          logger.warn("Fail to write to channel {}", channel);
        }
      }
    }
    return null;
  }
}
