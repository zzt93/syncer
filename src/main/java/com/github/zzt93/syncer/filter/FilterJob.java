package com.github.zzt93.syncer.filter;

import com.github.zzt93.syncer.common.Filter.FilterRes;
import com.github.zzt93.syncer.common.SyncData;
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
    while (!Thread.interrupted()) {
      try {
        SyncData poll = fromInput.take();
        for (ExprFilter filter : filters) {
          if (filter.decide(poll) == FilterRes.ACCEPT) {
            toOutput.offer(poll);
          }
        }
      } catch (Exception e) {
        logger.debug("Filter job failed", e);
      }
    }
    return null;
  }
}
