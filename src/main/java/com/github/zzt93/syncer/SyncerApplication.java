package com.github.zzt93.syncer;

import com.github.zzt93.syncer.common.thread.WaitingAckHook;
import com.github.zzt93.syncer.config.YamlEnvironmentPostProcessor;
import com.github.zzt93.syncer.config.pipeline.ConsumerConfig;
import com.github.zzt93.syncer.config.pipeline.ProducerConfig;
import com.github.zzt93.syncer.config.syncer.SyncerConfig;
import com.github.zzt93.syncer.consumer.ConsumerStarter;
import com.github.zzt93.syncer.health.SyncerHealth;
import com.github.zzt93.syncer.producer.ProducerStarter;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.Banner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;

import java.util.LinkedList;
import java.util.List;


@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class, MongoAutoConfiguration.class})
public class SyncerApplication implements CommandLineRunner {

  private static final Logger logger = LoggerFactory.getLogger(SyncerApplication.class);

  private final ProducerConfig producerConfig;
  private final SyncerConfig syncerConfig;
  private final ConsumerRegistry consumerRegistry;
  @Value("${syncer.version}")
  private String version;

  @Autowired
  public SyncerApplication(ProducerConfig producerConfig, SyncerConfig syncerConfig,
      ConsumerRegistry consumerRegistry) {
    this.producerConfig = producerConfig;
    this.syncerConfig = syncerConfig;
    this.consumerRegistry = consumerRegistry;
  }

  public static void main(String[] args) {
    try {
      new SpringApplicationBuilder()
        .sources(SyncerApplication.class)
        .web(WebApplicationType.SERVLET)
        .bannerMode(Banner.Mode.OFF)
        .properties()
        .run(args);
    } catch (Throwable e) {
      logger.error("", e);
      ShutDownCenter.initShutDown();
    }
  }

  @Override
  public void run(String... strings) throws Exception {
    List<Starter> starters = new LinkedList<>();
    for (ConsumerConfig consumerConfig : YamlEnvironmentPostProcessor.getConfigs()) {
      if (!validPipeline(consumerConfig)) {
        continue;
      }
      starters.add(new ConsumerStarter(consumerConfig, syncerConfig, consumerRegistry).start());
    }
    starters.add(ProducerStarter
        .getInstance(producerConfig.getInput(), syncerConfig.getInput(), consumerRegistry)
        .start());

    Runtime.getRuntime().addShutdownHook(new WaitingAckHook(starters));

    SyncerHealth.init(starters);
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
    return true;
  }

  private boolean supportedVersion(String version) {
    return version.equals(this.version);
  }

}
