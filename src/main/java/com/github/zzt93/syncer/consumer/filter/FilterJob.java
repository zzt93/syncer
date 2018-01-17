package com.github.zzt93.syncer.consumer.filter;

import com.github.zzt93.syncer.common.IdGenerator;
import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * @author zzt
 */
public class FilterJob implements Runnable {

  private final Logger logger = LoggerFactory.getLogger(FilterJob.class);
  private final BlockingDeque<SyncData> fromInput;
  private final List<OutputChannel> outputChannels;
  private final List<ExprFilter> filters;

  public FilterJob(BlockingDeque<SyncData> fromInput, List<OutputChannel> outputChannels,
      List<ExprFilter> filters) {
    this.fromInput = fromInput;
    this.outputChannels = outputChannels;
    this.filters = filters;
  }

  @Override
  public void run() {
    LinkedList<SyncData> list = new LinkedList<>();
    while (!Thread.interrupted()) {
      try {
        list.clear();
        SyncData poll = fromInput.take();
        MDC.put(IdGenerator.EID, poll.getEventId());
        list.add(poll);
        for (ExprFilter filter : filters) {
          filter.decide(list);
        }
      } catch (Exception e) {
        logger.debug("Filter job failed", e);
      }
      for (OutputChannel outputChannel : outputChannels) {
//        outputChannel.output()
      }
    }
  }
}
