package com.github.zzt93.syncer.consumer;

import com.github.zzt93.syncer.ShutDownCenter;
import com.github.zzt93.syncer.Starter;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncInitMeta;
import com.github.zzt93.syncer.common.util.NamedThreadFactory;
import com.github.zzt93.syncer.config.common.MasterSource;
import com.github.zzt93.syncer.config.consumer.ConsumerConfig;
import com.github.zzt93.syncer.config.consumer.filter.FilterConfig;
import com.github.zzt93.syncer.config.consumer.input.PipelineInput;
import com.github.zzt93.syncer.config.consumer.output.PipelineOutput;
import com.github.zzt93.syncer.config.syncer.SyncerAck;
import com.github.zzt93.syncer.config.syncer.SyncerConfig;
import com.github.zzt93.syncer.config.syncer.SyncerFilter;
import com.github.zzt93.syncer.config.syncer.SyncerFilterMeta;
import com.github.zzt93.syncer.config.syncer.SyncerInput;
import com.github.zzt93.syncer.config.syncer.SyncerOutput;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.ack.PositionFlusher;
import com.github.zzt93.syncer.consumer.filter.FilterJob;
import com.github.zzt93.syncer.consumer.output.OutputStarter;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;
import com.github.zzt93.syncer.data.util.SyncFilter;
import com.github.zzt93.syncer.health.Health;
import com.github.zzt93.syncer.health.SyncerHealth;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Abstraction of a consumer which is initiated by a pipeline config file
 *
 * @author zzt
 */
public class ConsumerStarter implements Starter {

  private final Logger logger = LoggerFactory.getLogger(ConsumerStarter.class);
  private final String id;
  private final List<OutputChannel> outputChannels;
  private ExecutorService filterService;
  private FilterJob filterJob;
  private SyncerAck ackConfig;
  private Registrant registrant;
  private Ack ack;
  private OutputStarter outputStarter;

  public ConsumerStarter(ConsumerConfig pipeline, SyncerConfig syncer,
                         ConsumerRegistry consumerRegistry) throws Exception {

    id = pipeline.getConsumerId();

    HashMap<String, SyncInitMeta> ackConnectionId2SyncInitMeta = initAckModule(id, pipeline.getInput(),
        syncer.getInput(), syncer.getAck(), pipeline.outputSize());

    outputChannels = initBatchOutputModule(id, pipeline.getOutput(), syncer.getOutput(), ack);

    ArrayBlockingQueue<SyncData> inputFilterQuery = new ArrayBlockingQueue<>(syncer.getFilter().getCapacity());
    initFilterModule(syncer.getFilter(), pipeline.getFilter(), outputChannels, ack, inputFilterQuery);

    initRegistrant(id, consumerRegistry, inputFilterQuery, pipeline.getInput(), ackConnectionId2SyncInitMeta);
  }

  private HashMap<String, SyncInitMeta> initAckModule(String consumerId,
                                                      PipelineInput pipelineInput,
                                                      SyncerInput input, SyncerAck ackConfig, int outputSize) {
    Set<MasterSource> masterSet = pipelineInput.getMasterSet();
    HashMap<String, SyncInitMeta> ackConnectionId2SyncInitMeta = new HashMap<>();
    this.ack = Ack.build(consumerId, input.getInputMeta(), masterSet, ackConnectionId2SyncInitMeta, outputSize);
    this.ackConfig = ackConfig;
    return ackConnectionId2SyncInitMeta;
  }

  private List<OutputChannel> initBatchOutputModule(String id, PipelineOutput pipeline,
                                                    SyncerOutput syncer, Ack ack) throws Exception {
    outputStarter = new OutputStarter(id, pipeline, syncer, ack);
    return outputStarter.getOutputChannels();
  }

  private void initFilterModule(SyncerFilter module, List<FilterConfig> filters,
                                List<OutputChannel> outputChannels, Ack ack, ArrayBlockingQueue<SyncData> inputFilterQuery) {

    filterService = Executors
        .newFixedThreadPool(SyncerFilter.WORKER_THREAD_COUNT, new NamedThreadFactory("syncer-" + id + "-filter"));

    List<SyncFilter> syncFilters = fromPipelineConfig(filters, module);
    // this list shared by multiple thread, and some channels may be removed when other threads iterate
    // see CopyOnWriteListTest for sanity test
    CopyOnWriteArrayList<OutputChannel> channels = new CopyOnWriteArrayList<>(outputChannels);
    filterJob = new FilterJob(id, inputFilterQuery, channels, syncFilters, ack, module);
  }

  private void initRegistrant(String consumerId, ConsumerRegistry consumerRegistry,
                              ArrayBlockingQueue<SyncData> inputFilterQueue,
                              PipelineInput input,
                              HashMap<String, SyncInitMeta> ackConnectionId2SyncInitMeta) {
    registrant = new Registrant(consumerRegistry);
    for (MasterSource masterSource : input.getMasterSet()) {
      List<? extends ConsumerSource> localConsumerSources =
          masterSource.toConsumerSources(consumerId, ack, ackConnectionId2SyncInitMeta, inputFilterQueue);
      registrant.addDatasource(localConsumerSources);
    }
  }

  private List<SyncFilter> fromPipelineConfig(List<FilterConfig> filters, SyncerFilter syncerFilter) {
    SyncerFilterMeta filterMeta = syncerFilter.getFilterMeta();
    SpelExpressionParser parser = new SpelExpressionParser();
    List<SyncFilter> res = new ArrayList<>();
    for (FilterConfig filter : filters) {
      res.add(filter.addMeta(id, filterMeta).toFilter(parser));
    }
    return res;
  }

  public Starter start() throws InterruptedException, IOException {
    startAck();
    filterService.submit(filterJob);
    return this;
  }

  public void close() throws InterruptedException {
    // close output channel first
    outputStarter.close();
    // stop filter-output threads
    filterService.shutdownNow();
    while (!filterService.awaitTermination(ShutDownCenter.SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)) {
      logger.error("[Shutting down] consumer: {}", id);
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
        .newScheduledThreadPool(1, new NamedThreadFactory("syncer-" + id + "-ack"));
    if (registrant.register()) {
      scheduled.scheduleAtFixedRate(new PositionFlusher(ack), 0, ackConfig.getFlushPeriod(),
          ackConfig.getUnit());
    } else {
      logger.warn("Fail to register");
    }
  }
}
