package com.github.zzt93.syncer.consumer.filter;

import com.github.zzt93.syncer.common.IdGenerator;
import com.github.zzt93.syncer.common.SyncData;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * @author zzt
 */
public class FilterJob implements Runnable {

  private final Logger logger = LoggerFactory.getLogger(FilterJob.class);
  private final BlockingDeque<SyncData> fromInput;
  private final BlockingQueue<SyncData> toOutput;
  private final List<ExprFilter> filters;

  public FilterJob(BlockingDeque<SyncData> fromInput, BlockingQueue<SyncData> toOutput,
      List<ExprFilter> filters) {
    this.fromInput = fromInput;
    this.toOutput = toOutput;
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
        // TODO 18/1/12 replace with template method to add MDC
        list.add(poll);
        for (ExprFilter filter : filters) {
          filter.decide(list);
        }
      } catch (Exception e) {
        logger.debug("Filter job failed", e);
      }
      toOutput.addAll(list);
    }
  }
}
