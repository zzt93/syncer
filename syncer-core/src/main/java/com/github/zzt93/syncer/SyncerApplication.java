package com.github.zzt93.syncer;

import com.github.zzt93.syncer.common.thread.WaitingAckHook;
import com.github.zzt93.syncer.common.util.RegexUtil;
import com.github.zzt93.syncer.config.CmdProcessor;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.consumer.ConsumerConfig;
import com.github.zzt93.syncer.config.consumer.ProducerConfig;
import com.github.zzt93.syncer.config.syncer.SyncerConfig;
import com.github.zzt93.syncer.consumer.ConsumerInitContext;
import com.github.zzt93.syncer.consumer.ConsumerStarter;
import com.github.zzt93.syncer.health.SyncerHealth;
import com.github.zzt93.syncer.health.export.ExportServer;
import com.github.zzt93.syncer.producer.ProducerStarter;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import com.github.zzt93.syncer.stat.SyncerInfo;
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
  private final SyncerInfo syncerInfo;

  public SyncerApplication(ProducerConfig producerConfig, SyncerConfig syncerConfig,
                           ConsumerRegistry consumerRegistry, List<ConsumerConfig> consumerConfigs, SyncerInfo info) {
    this.producerConfig = producerConfig;
    this.syncerConfig = syncerConfig;
    this.consumerRegistry = consumerRegistry;
    this.consumerConfigs = consumerConfigs;
    this.syncerInfo = info;
  }

  public static void main(String[] args) {
    try {
      SyncerApplication syncer = CmdProcessor.processCmdArgs(args);
      syncer.run();
    } catch (Throwable e) {
      ShutDownCenter.initShutDown(e);
    }
  }

  public void run() throws Exception {
    LinkedList<Starter> starters = new LinkedList<>();
    HashSet<String> consumerIds = new HashSet<>();
    for (ConsumerConfig consumerConfig : consumerConfigs) {
      if (!validPipeline(consumerConfig)) {
        throw new InvalidConfigException("Invalid consumer config");
      }
      if (consumerIds.contains(consumerConfig.getConsumerId())) {
        throw new InvalidConfigException("Duplicate consumerId: " + consumerConfig.getConsumerId());
      }
      consumerIds.add(consumerConfig.getConsumerId());
      ConsumerInitContext consumerInitContext = new ConsumerInitContext(syncerInfo, syncerConfig, consumerConfig);
      starters.add(new ConsumerStarter(consumerRegistry, consumerInitContext).start());
    }
    // add producer as first item, stop producer first
    starters.addFirst(ProducerStarter
        .getInstance(producerConfig.getInput(), syncerConfig.getInput(), consumerRegistry)
        .start());

    Runtime.getRuntime().addShutdownHook(new WaitingAckHook(starters));

    SyncerHealth.init(starters);
    ExportServer.init(getSyncerConfig());
  }

  private boolean validPipeline(ConsumerConfig consumerConfig) {
    if (!supportedVersion(consumerConfig.getVersion())) {
      logger.error("Not supported version[{}] config file, current version is {}", consumerConfig.getVersion(), syncerInfo);
      return false;
    }
    String consumerId = consumerConfig.getConsumerId();
    if (consumerId == null) {
      logger.error("No `consumerId` specified");
      return false;
    }
    if (!RegexUtil.isClassName(consumerId)) {
      logger.error("`consumerId: {}` not match Java identifier format: [a-zA-Z_$][a-zA-Z\\d_$]*", consumerId);
      return false;
    }
    return true;
  }

  private boolean supportedVersion(String version) {
    return version.equals(this.syncerInfo.getVersion());
  }

  public SyncerConfig getSyncerConfig() {
    return syncerConfig;
  }
}
