package com.github.zzt93.syncer.consumer;

import com.github.zzt93.syncer.ShutDownCenter;
import com.github.zzt93.syncer.Starter;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncInitMeta;
import com.github.zzt93.syncer.common.util.NamedThreadFactory;
import com.github.zzt93.syncer.config.common.MasterSource;
import com.github.zzt93.syncer.config.consumer.filter.FilterConfig;
import com.github.zzt93.syncer.config.consumer.input.PipelineInput;
import com.github.zzt93.syncer.config.syncer.SyncerAck;
import com.github.zzt93.syncer.config.syncer.SyncerFilter;
import com.github.zzt93.syncer.config.syncer.SyncerFilterMeta;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;

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

  public ConsumerStarter(ConsumerRegistry consumerRegistry, ConsumerInitContext consumerInitContext) throws Exception {
    id = consumerInitContext.getConsumerId();

    HashMap<String, SyncInitMeta> ackConnectionId2SyncInitMeta = initAckModule(consumerInitContext);

    outputChannels = initBatchOutputModule(consumerInitContext, ack);

    ArrayBlockingQueue<SyncData> inputFilterQueue = new ArrayBlockingQueue<>(consumerInitContext.getSyncerFilter().getCapacity());
    initFilterModule(consumerInitContext, outputChannels, ack, inputFilterQueue);

    initRegistrant(id, consumerRegistry, inputFilterQueue, consumerInitContext.getInput(), ackConnectionId2SyncInitMeta);
  }

  private HashMap<String, SyncInitMeta> initAckModule(ConsumerInitContext context) {
    HashMap<String, SyncInitMeta> ackConnectionId2SyncInitMeta = new HashMap<>();
    this.ack = Ack.build(context, ackConnectionId2SyncInitMeta);
    this.ackConfig = context.getAck();
    return ackConnectionId2SyncInitMeta;
  }

  private List<OutputChannel> initBatchOutputModule(ConsumerInitContext initContext, Ack ack) throws Exception {
    outputStarter = new OutputStarter(initContext.getConsumerId(), initContext.getOutput(), initContext.getSyncerOutput(), ack);
    return outputStarter.getOutputChannels();
  }

  private void initFilterModule(ConsumerInitContext initContext,
                                List<OutputChannel> outputChannels, Ack ack, ArrayBlockingQueue<SyncData> inputFilterQuery) {

    filterService = Executors
        .newFixedThreadPool(SyncerFilter.WORKER_THREAD_COUNT, new NamedThreadFactory("syncer-" + id + "-filter"));

    List<SyncFilter> syncFilters = fromPipelineConfig(initContext.getFilter(), initContext.getSyncerFilter());
    // this list shared by multiple thread, and some channels may be removed when other threads iterate
    // see CopyOnWriteListTest for sanity test
    CopyOnWriteArrayList<OutputChannel> channels = new CopyOnWriteArrayList<>(outputChannels);
    filterJob = new FilterJob(id, inputFilterQuery, channels, syncFilters, ack, initContext.getSyncerFilter());
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

  private List<SyncFilter> fromPipelineConfig(FilterConfig filter, SyncerFilter syncerFilter) {
    SyncerFilterMeta filterMeta = syncerFilter.getFilterMeta();
    List<SyncFilter> res = new ArrayList<>();
    SyncFilter e = filter.addMeta(id, filterMeta).toFilter();
    if (e != null) {
      res.add(e);
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
