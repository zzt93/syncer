package com.github.zzt93.syncer.consumer.filter;

import com.github.zzt93.syncer.Starter;
import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.util.NamedThreadFactory;
import com.github.zzt93.syncer.config.pipeline.filter.FilterConfig;
import com.github.zzt93.syncer.config.syncer.SyncerFilter;
import com.github.zzt93.syncer.consumer.filter.impl.ForeachFilter;
import com.github.zzt93.syncer.consumer.filter.impl.If;
import com.github.zzt93.syncer.consumer.filter.impl.Statement;
import com.github.zzt93.syncer.consumer.filter.impl.Switch;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * @author zzt
 */
public class FilterStarter implements Starter<List<FilterConfig>, List<ExprFilter>> {

  private ExecutorService service;
  private FilterJob filterJob;
  private int worker;

  public FilterStarter(List<FilterConfig> pipeline,
      SyncerFilter filter, BlockingDeque<SyncData> fromInput,
      BlockingQueue<SyncData> toOutput) {
    List<ExprFilter> filterJobs = fromPipelineConfig(pipeline);
    filterModuleInit(filter, filterJobs, fromInput, toOutput);
  }

  private void filterModuleInit(SyncerFilter module, List<ExprFilter> filters,
      BlockingDeque<SyncData> fromInput, BlockingQueue<SyncData> toOutput) {
    Preconditions.checkArgument(module.getWorker() <= 8, "Too many worker thread");
    Preconditions.checkArgument(module.getWorker() > 0, "Too few worker thread");
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
        case IF:
          res.add(new If(parser, filter.getIf()));
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
