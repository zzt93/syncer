package com.github.zzt93.syncer.config.consumer;

import com.github.zzt93.syncer.config.producer.ProducerInput;

/**
 * @author zzt
 */
public class ProducerConfig {

  private String version;
  private ProducerInput input;

  public ProducerInput getInput() {
    return input;
  }

  public void setInput(ProducerInput input) {
    this.input = input;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }
}
