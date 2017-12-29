package com.github.zzt93.syncer.filter;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.event.RowsEvent;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * @author zzt
 */
public class FilterJob implements Callable<Void> {

  private final Logger logger = LoggerFactory.getLogger(FilterJob.class);
  private final BlockingQueue<SyncData> fromInput;
  private final BlockingQueue<SyncData> toOutput;
  private final List<ExprFilter> filters;

  public FilterJob(BlockingQueue<SyncData> fromInput, BlockingQueue<SyncData> toOutput,
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
        SyncData poll = fromInput.take();
        MDC.put(RowsEvent.EID, poll.getEventId());
        list.add(poll);
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
