package com.github.zzt93.syncer.consumer.filter;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.consumer.InputSource;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class FilterJob implements Callable<Void> {

  private final Logger logger = LoggerFactory.getLogger(FilterJob.class);
  private final List<InputSource> fromInput;
  private final BlockingQueue<SyncData> toOutput;
  private final List<ExprFilter> filters;

  public FilterJob(List<InputSource> fromInput, BlockingQueue<SyncData> toOutput,
      List<ExprFilter> filters) {
    this.fromInput = fromInput;
    this.toOutput = toOutput;
    this.filters = filters;
  }

  @Override
  public Void call() throws Exception {
    LinkedList<SyncData> list = new LinkedList<>();
    while (!Thread.interrupted()) {
      try {
        list.clear();
//        SyncData poll = fromInput.take();
//        // TODO 18/1/12 replace with template method to add MDC
//        MDC.put(RowsEvent.EID, poll.getEventId());
//        list.add(poll);
        for (ExprFilter filter : filters) {
          filter.decide(list);
        }
      } catch (Exception e) {
        logger.debug("Filter job failed", e);
      }
      toOutput.addAll(list);
    }
    return null;
  }
}
