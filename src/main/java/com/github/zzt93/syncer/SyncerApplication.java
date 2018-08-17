package com.github.zzt93.syncer;

import com.github.zzt93.syncer.config.YamlEnvironmentPostProcessor;
import com.github.zzt93.syncer.config.pipeline.PipelineConfig;
import com.github.zzt93.syncer.config.pipeline.ProducerConfig;
import com.github.zzt93.syncer.config.syncer.SyncerConfig;
import com.github.zzt93.syncer.consumer.ConsumerStarter;
import com.github.zzt93.syncer.producer.ProducerStarter;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.Banner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;


@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class, MongoAutoConfiguration.class})
public class SyncerApplication implements CommandLineRunner {

  private final Logger logger = LoggerFactory.getLogger(SyncerApplication.class);

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
    SpringApplication application = new SpringApplication(SyncerApplication.class);
    application.setWebApplicationType(WebApplicationType.NONE);
    application.setBannerMode(Banner.Mode.OFF);
    application.run(args);
  }

  @Override
  public void run(String... strings) throws Exception {
    for (PipelineConfig pipelineConfig : YamlEnvironmentPostProcessor.getConfigs()) {
      if (!validPipeline(pipelineConfig)) {
        continue;
      }
      new ConsumerStarter(pipelineConfig, syncerConfig, consumerRegistry).start();
    }
    ProducerStarter
        .getInstance(producerConfig.getInput(), syncerConfig.getInput(), consumerRegistry)
        .start()
        .waitTermination();
  }

  private boolean validPipeline(PipelineConfig pipelineConfig) {
    if (!supportedVersion(pipelineConfig.getVersion())) {
      logger.error("Not supported version[{}] config file", pipelineConfig.getVersion());
      return false;
    }
    String consumerId = pipelineConfig.getConsumerId();
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
