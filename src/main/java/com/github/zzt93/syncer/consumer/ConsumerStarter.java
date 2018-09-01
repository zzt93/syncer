package com.github.zzt93.syncer.consumer;

import com.github.zzt93.syncer.Starter;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncInitMeta;
import com.github.zzt93.syncer.common.util.NamedThreadFactory;
import com.github.zzt93.syncer.config.pipeline.PipelineConfig;
import com.github.zzt93.syncer.config.pipeline.common.MasterSource;
import com.github.zzt93.syncer.config.pipeline.filter.FilterConfig;
import com.github.zzt93.syncer.config.pipeline.input.PipelineInput;
import com.github.zzt93.syncer.config.pipeline.input.SyncMeta;
import com.github.zzt93.syncer.config.pipeline.output.PipelineOutput;
import com.github.zzt93.syncer.config.syncer.*;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.filter.ExprFilter;
import com.github.zzt93.syncer.consumer.filter.FilterJob;
import com.github.zzt93.syncer.consumer.filter.impl.ForeachFilter;
import com.github.zzt93.syncer.consumer.filter.impl.If;
import com.github.zzt93.syncer.consumer.filter.impl.Statement;
import com.github.zzt93.syncer.consumer.filter.impl.Switch;
import com.github.zzt93.syncer.consumer.input.*;
import com.github.zzt93.syncer.consumer.output.OutputStarter;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;
import com.github.zzt93.syncer.health.Health;
import com.github.zzt93.syncer.health.SyncerHealth;
import com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Abstraction of a consumer which is initiated by a pipeline config file
 * @author zzt
 */
public class ConsumerStarter implements Starter<List<FilterConfig>, List<ExprFilter>> {

  private final Logger logger = LoggerFactory.getLogger(ConsumerStarter.class);
  private ExecutorService service;
  private FilterJob[] filterJobs;
  private int worker;
  private SyncerAck ackConfig;
  private Registrant registrant;
  private Ack ack;
  private final String id;
  private final List<OutputChannel> outputChannels;

  public ConsumerStarter(PipelineConfig pipeline, SyncerConfig syncer,
      ConsumerRegistry consumerRegistry) throws Exception {

    id = pipeline.getConsumerId();
    HashMap<String, SyncInitMeta> id2SyncInitMeta = initAckModule(id, pipeline.getInput(),
        syncer.getInput(), syncer.getAck());

    outputChannels = initBatchOutputModule(pipeline.getOutput(), syncer.getOutput());

    SchedulerBuilder schedulerBuilder = new SchedulerBuilder();
    initFilterModule(ack, syncer.getFilter(), pipeline.getFilter(), schedulerBuilder, outputChannels);

    initRegistrant(id, consumerRegistry, schedulerBuilder, pipeline.getInput(), id2SyncInitMeta);
  }

  private HashMap<String, SyncInitMeta> initAckModule(String consumerId,
      PipelineInput pipelineInput,
      SyncerInput input, SyncerAck ackConfig) {
    Set<MasterSource> masterSet = pipelineInput.getMasterSet();
    HashMap<String, SyncInitMeta> id2SyncInitMeta = new HashMap<>();
    this.ack = Ack.build(consumerId, input.getInputMeta(), masterSet, id2SyncInitMeta);
    this.ackConfig = ackConfig;
    return id2SyncInitMeta;
  }

  private List<OutputChannel> initBatchOutputModule(PipelineOutput pipeline, SyncerOutput syncer)
      throws Exception {
    return new OutputStarter(pipeline, syncer, ack).getOutputChannels();
  }

  private void initFilterModule(Ack ack, SyncerFilter module, List<FilterConfig> filters,
      SchedulerBuilder schedulerBuilder, List<OutputChannel> outputChannels) {
    Preconditions
        .checkArgument(module.getWorker() <= Runtime.getRuntime().availableProcessors() * 3,
            "Too many worker thread");
    Preconditions.checkArgument(module.getWorker() > 0, "Invalid worker thread number config");
    service = Executors
        .newFixedThreadPool(module.getWorker(), new NamedThreadFactory("syncer-filter-output"));

    List<ExprFilter> exprFilters = fromPipelineConfig(filters);
    worker = module.getWorker();
    filterJobs = new FilterJob[worker];
    BlockingDeque<SyncData>[] deques = new BlockingDeque[worker];
    for (int i = 0; i < worker; i++) {
      deques[i] = new LinkedBlockingDeque<>();
      // TODO 18/8/15 new list?
      filterJobs[i] = new FilterJob(id, ack, deques[i], new CopyOnWriteArrayList<>(outputChannels),
          exprFilters);
    }
    schedulerBuilder.setDeques(deques);
  }

  private void initRegistrant(String consumerId, ConsumerRegistry consumerRegistry,
      SchedulerBuilder schedulerBuilder,
      PipelineInput input,
      HashMap<String, SyncInitMeta> id2SyncInitMeta) {
    registrant = new Registrant(consumerRegistry);
    for (MasterSource masterSource : input.getMasterSet()) {
      String identifier = masterSource.getConnection().connectionIdentifier();
      SyncInitMeta syncInitMeta = id2SyncInitMeta.get(identifier);
      if (masterSource.hasSyncMeta()) {
        SyncMeta syncMeta = masterSource.getSyncMeta();
        logger.info("Override syncer remembered position with config in file {}, watch out",
            syncMeta);
        syncInitMeta = new BinlogInfo(syncMeta.getBinlogFilename(), syncMeta.getBinlogPosition());
      }
      EventScheduler scheduler = schedulerBuilder.setSchedulerType(masterSource.getScheduler())
          .build();
      LocalInputSource localInputSource = LocalInputSource
          .inputSource(consumerId, masterSource, syncInitMeta, scheduler);
      registrant.addDatasource(localInputSource);
    }
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

  public Starter start() throws InterruptedException, IOException {
    startAck();
    for (int i = 0; i < worker; i++) {
      service.submit(filterJobs[i]);
    }
    return this;
  }

  public void close() throws InterruptedException {
    service.shutdownNow();
    if(!service.awaitTermination(5, TimeUnit.SECONDS)) {
      logger.error("Fail to shutdown consumer: {}", id);
    }
  }

  @Override
  public void registerToHealthCenter() {
    for (OutputChannel outputChannel : outputChannels) {
      SyncerHealth.consumer(id, outputChannel.id(), Health.green());
    }
  }

  private void startAck() throws IOException {
    ScheduledExecutorService scheduled = Executors
        .newScheduledThreadPool(1, new NamedThreadFactory("syncer-ack"));
    if (registrant.register()) {
      scheduled.scheduleAtFixedRate(new PositionFlusher(ack), 0, ackConfig.getFlushPeriod(),
          ackConfig.getUnit());
    } else {
      logger.warn("Fail to register");
    }
  }
}
