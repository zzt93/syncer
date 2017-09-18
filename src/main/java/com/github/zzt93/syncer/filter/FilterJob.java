package com.github.zzt93.syncer.filter;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.filter.Filter.FilterRes;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

/**
 * @author zzt
 */
public class FilterJob implements Callable<Void> {


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
    while (true) {
      SyncData poll = fromInput.poll();
      for (ExprFilter filter : filters) {
        if (filter.decide(poll) == FilterRes.ACCEPT) {
          toOutput.offer(poll);
        }
      }
    }
  }
}
