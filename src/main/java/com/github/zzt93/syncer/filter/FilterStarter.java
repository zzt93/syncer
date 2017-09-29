package com.github.zzt93.syncer.filter;

import com.github.zzt93.syncer.Starter;
import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.util.NamedThreadFactory;
import com.github.zzt93.syncer.config.pipeline.filter.FilterConfig;
import com.github.zzt93.syncer.config.syncer.SyncerFilter;
import com.github.zzt93.syncer.filter.impl.ForeachFilter;
import com.github.zzt93.syncer.filter.impl.Statement;
import com.github.zzt93.syncer.filter.impl.Switch;
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
public class FilterStarter implements Starter<List<FilterConfig>, List<ExprFilter>> {

  private static FilterStarter instance;
  private ExecutorService service;
  private FilterJob filterJob;
  private int worker;

  private FilterStarter(List<FilterConfig> pipeline,
      SyncerFilter filter, BlockingQueue<SyncData> fromInput,
      BlockingQueue<SyncData> toOutput) {
    List<ExprFilter> filterJobs = fromPipelineConfig(pipeline);
    filterModuleInit(filter, filterJobs, fromInput, toOutput);
  }

  public static FilterStarter getInstance(List<FilterConfig> filters,
      SyncerFilter filter, BlockingQueue<SyncData> fromInput,
      BlockingQueue<SyncData> toOutput) {
    if (instance == null) {
      instance = new FilterStarter(filters, filter, fromInput, toOutput);
    }
    return instance;
  }

  private void filterModuleInit(SyncerFilter module, List<ExprFilter> filters,
      BlockingQueue<SyncData> fromInput, BlockingQueue<SyncData> toOutput) {
    Assert.isTrue(module.getWorker() <= 8, "Too many worker thread");
    Assert.isTrue(module.getWorker() > 0, "Too few worker thread");
    service = Executors
        .newFixedThreadPool(module.getWorker(), new NamedThreadFactory("syncer-filter"));

    filterJob = new FilterJob(fromInput, toOutput, filters);
    worker = module.getWorker();
  }

  @Override
  public List<ExprFilter> fromPipelineConfig(List<FilterConfig> filters) {
    SpelExpressionParser parser = new SpelExpressionParser();
    List<ExprFilter> res = new ArrayList<>();
    for (FilterConfig filter : filters) {
      switch (filter.getType()) {
        case SWITCH:
          res.add(new Switch(parser, filter.getSwitcher()));
          break;
        case STATEMENT:
          res.add(new Statement(parser, filter.getStatement()));
          break;
        case FOREACH:
          res.add(new ForeachFilter(parser, filter.getForeach()));
          break;
        default:
          throw new IllegalArgumentException("Unknown filter type");
      }
    }
    return res;
  }

  public void start() throws InterruptedException {
    for (int i = 0; i < worker; i++) {
      service.submit(filterJob);
    }
  }
}
