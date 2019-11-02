package com.github.zzt93.syncer;

import com.github.zzt93.syncer.common.thread.WaitingAckHook;
import com.github.zzt93.syncer.common.util.RegexUtil;
import com.github.zzt93.syncer.config.YamlEnvironmentPostProcessor;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.consumer.ConsumerConfig;
import com.github.zzt93.syncer.config.consumer.ProducerConfig;
import com.github.zzt93.syncer.config.syncer.SyncerConfig;
import com.github.zzt93.syncer.consumer.ConsumerStarter;
import com.github.zzt93.syncer.health.SyncerHealth;
import com.github.zzt93.syncer.health.export.ExportServer;
import com.github.zzt93.syncer.producer.ProducerStarter;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;


public class SyncerApplication {

  private static final Logger logger = LoggerFactory.getLogger(SyncerApplication.class);

  private final ProducerConfig producerConfig;
  private final SyncerConfig syncerConfig;
  private final ConsumerRegistry consumerRegistry;
  private final List<ConsumerConfig> consumerConfigs;
  private final String version;

  public SyncerApplication(ProducerConfig producerConfig, SyncerConfig syncerConfig,
                           ConsumerRegistry consumerRegistry, List<ConsumerConfig> consumerConfigs, String version) {
    this.producerConfig = producerConfig;
    this.syncerConfig = syncerConfig;
    this.consumerRegistry = consumerRegistry;
    this.consumerConfigs = consumerConfigs;
    this.version = version;
  }

  public static void main(String[] args) {
    try {
      SyncerApplication syncer = YamlEnvironmentPostProcessor.processEnvironment(args);
      syncer.run(args);
    } catch (Throwable e) {
      ShutDownCenter.initShutDown(e);
    }
  }

  public void run(String[] args) throws Exception {
    LinkedList<Starter> starters = new LinkedList<>();
    HashSet<String> consumerIds = new HashSet<>();
    for (ConsumerConfig consumerConfig : consumerConfigs) {
      if (!validPipeline(consumerConfig)) {
        continue;
      }
      if (consumerIds.contains(consumerConfig.getConsumerId())) {
        throw new InvalidConfigException("Duplicate consumerId: " + consumerConfig.getConsumerId());
      }
      consumerIds.add(consumerConfig.getConsumerId());
      starters.add(new ConsumerStarter(consumerConfig, syncerConfig, consumerRegistry).start());
    }
    // add producer as first item, stop producer first
    starters.addFirst(ProducerStarter
        .getInstance(producerConfig.getInput(), syncerConfig.getInput(), consumerRegistry)
        .start());

    Runtime.getRuntime().addShutdownHook(new WaitingAckHook(starters));

    SyncerHealth.init(starters);
    ExportServer.init(args);
  }

  private boolean validPipeline(ConsumerConfig consumerConfig) {
    if (!supportedVersion(consumerConfig.getVersion())) {
      logger.error("Not supported version[{}] config file", consumerConfig.getVersion());
      return false;
    }
    String consumerId = consumerConfig.getConsumerId();
    if (consumerId == null) {
      logger.error("No `consumerId` specified");
      return false;
    }
    if (!RegexUtil.isClassName(consumerId)) {
      logger.error("`consumerId` not match Java identifier regex: [a-zA-Z_$][a-zA-Z\\d_$]*");
      return false;
    }
    return true;
  }

  private boolean supportedVersion(String version) {
    return version.equals(this.version);
  }

}
