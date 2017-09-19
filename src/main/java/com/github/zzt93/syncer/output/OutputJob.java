package com.github.zzt93.syncer.output;

import com.github.zzt93.syncer.common.SyncData;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

/**
 * @author zzt
 */
public class OutputJob implements Callable<Void> {

  private final BlockingQueue<SyncData> queue;
  private final List<OutputChannel> channels;

  public OutputJob(BlockingQueue<SyncData> fromFilter, List<OutputChannel> output) {
    this.queue = fromFilter;
    this.channels = output;
  }

  @Override
  public Void call() {
    while (!Thread.interrupted()) {
      SyncData poll = queue.poll();
      for (OutputChannel channel : channels) {
        channel.output(poll);
      }
    }
    return null;
  }
}
