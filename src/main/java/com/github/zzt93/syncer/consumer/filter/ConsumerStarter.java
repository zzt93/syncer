package com.github.zzt93.syncer.consumer.filter;

import com.github.zzt93.syncer.Starter;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.util.NamedThreadFactory;
import com.github.zzt93.syncer.config.pipeline.filter.FilterConfig;
import com.github.zzt93.syncer.config.pipeline.output.PipelineOutput;
import com.github.zzt93.syncer.config.syncer.SyncerFilter;
import com.github.zzt93.syncer.config.syncer.SyncerOutput;
import com.github.zzt93.syncer.consumer.filter.impl.ForeachFilter;
import com.github.zzt93.syncer.consumer.filter.impl.If;
import com.github.zzt93.syncer.consumer.filter.impl.Statement;
import com.github.zzt93.syncer.consumer.filter.impl.Switch;
import com.github.zzt93.syncer.consumer.input.Ack;
import com.github.zzt93.syncer.consumer.output.OutputStarter;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * @author zzt
 */
public class ConsumerStarter implements Starter<List<FilterConfig>, List<ExprFilter>> {

  private ExecutorService service;
  private FilterJob filterJob;
  private int worker;

  public ConsumerStarter(Ack ack, List<FilterConfig> pipeline,
      SyncerFilter filter, BlockingDeque<SyncData> fromInput,
      PipelineOutput output,
      SyncerOutput syncerConfigOutput) throws Exception {
    List<ExprFilter> filterJobs = fromPipelineConfig(pipeline);
    List<OutputChannel> outputChannels = new OutputStarter(output, syncerConfigOutput, ack)
        .getOutputChannels();
    filterModuleInit(ack, filter, filterJobs, fromInput, outputChannels);
  }

  private void filterModuleInit(Ack ack, SyncerFilter module, List<ExprFilter> filters,
      BlockingDeque<SyncData> fromInput, List<OutputChannel> outputChannels) {
    Preconditions.checkArgument(module.getWorker() <= 8, "Too many worker thread");
    Preconditions.checkArgument(module.getWorker() > 0, "Too few worker thread");
    service = Executors
        .newFixedThreadPool(module.getWorker(), new NamedThreadFactory("syncer-filter-output"));

    filterJob = new FilterJob(ack, fromInput, outputChannels, filters);
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
