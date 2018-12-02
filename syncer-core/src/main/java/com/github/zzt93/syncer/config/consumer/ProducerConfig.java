package com.github.zzt93.syncer.config.consumer;

import com.github.zzt93.syncer.config.producer.ProducerInput;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author zzt
 */
@Configuration
@ConfigurationProperties("syncer.producer")
public class ProducerConfig {

  private ProducerInput input;

  public ProducerInput getInput() {
    return input;
  }

  public void setInput(ProducerInput input) {
    this.input = input;
  }

}
