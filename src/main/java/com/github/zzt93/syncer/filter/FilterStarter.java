package com.github.zzt93.syncer.filter;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.config.pipeline.filter.FilterConfig;
import com.github.zzt93.syncer.config.syncer.FilterModule;
import com.github.zzt93.syncer.filter.impl.Switch;
import com.github.zzt93.syncer.input.connect.NamedThreadFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Assert;

/**
 * @author zzt
 */
public class FilterStarter {

  private static FilterStarter instance;
  private ExecutorService service;
  private FilterJob filterJob;

  private FilterStarter(List<FilterConfig> pipeline,
      FilterModule filter, BlockingQueue<SyncData> fromInput,
      BlockingQueue<SyncData> toOutput) {
    List<ExprFilter> filterJobs = fromPipelineConfig(pipeline);
    filterModuleInit(filter, filterJobs, fromInput, toOutput);
  }

  public static FilterStarter getInstance(List<FilterConfig> filters,
      FilterModule filter, BlockingQueue<SyncData> fromInput,
      BlockingQueue<SyncData> toOutput) {
    if (instance == null) {
      instance = new FilterStarter(filters, filter, fromInput, toOutput);
    }
    return instance;
  }

  private void filterModuleInit(FilterModule module, List<ExprFilter> filters,
      BlockingQueue<SyncData> fromInput, BlockingQueue<SyncData> toOutput) {
    Assert.isTrue(module.getWorker() <= 8, "Too many worker thread");
    Assert.isTrue(module.getWorker() > 0, "Too few worker thread");
    service = Executors
        .newFixedThreadPool(module.getWorker(), new NamedThreadFactory("syncer-filter"));

    filterJob = new FilterJob(fromInput, toOutput, filters);
  }

  private List<ExprFilter> fromPipelineConfig(List<FilterConfig> filters) {
    SpelExpressionParser parser = new SpelExpressionParser();
    List<ExprFilter> res = new ArrayList<>();
    for (FilterConfig filter : filters) {
      res.add(new Switch(parser, filter));
    }
    return res;
  }

  public void start() throws InterruptedException {
    service.submit(filterJob);
  }
}
