package com.github.zzt93.syncer;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.config.YamlEnvironmentPostProcessor;
import com.github.zzt93.syncer.config.pipeline.PipelineConfig;
import com.github.zzt93.syncer.config.pipeline.ProducerConfig;
import com.github.zzt93.syncer.config.syncer.SyncerConfig;
import com.github.zzt93.syncer.consumer.filter.FilterStarter;
import com.github.zzt93.syncer.consumer.input.RegistrationStarter;
import com.github.zzt93.syncer.consumer.output.OutputStarter;
import com.github.zzt93.syncer.producer.ProducerStarter;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.Banner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;


@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class SyncerApplication implements CommandLineRunner {

  @Autowired
  private YamlEnvironmentPostProcessor yamlEnvProcessor;
  @Autowired
  private ProducerConfig producerConfig;
  @Autowired
  private SyncerConfig syncerConfig;
  @Autowired
  private ConsumerRegistry consumerRegistry;

  public static void main(String[] args) {
    SpringApplication application = new SpringApplication(SyncerApplication.class);
    application.setWebApplicationType(WebApplicationType.NONE);
    application.setBannerMode(Banner.Mode.OFF);
    application.run(args);
  }

  @Override
  public void run(String... strings) throws Exception {
    int consumerId = 0;
    for (PipelineConfig pipelineConfig : yamlEnvProcessor.getConfigs()) {
      BlockingDeque<SyncData> filterInput = new LinkedBlockingDeque<>();
      BlockingDeque<SyncData> filterOutput = new LinkedBlockingDeque<>();
      new RegistrationStarter(pipelineConfig.getInput(),
          syncerConfig.getInput(), consumerRegistry, consumerId++, filterInput).start();
      new FilterStarter(pipelineConfig.getFilter(), syncerConfig.getFilter(), filterInput, filterOutput).start();
      new OutputStarter(pipelineConfig.getOutput(), syncerConfig.getOutput(), filterOutput).start();
    }
    ProducerStarter.getInstance(producerConfig.getInput(), syncerConfig.getInput(), consumerRegistry).start();
  }

}
