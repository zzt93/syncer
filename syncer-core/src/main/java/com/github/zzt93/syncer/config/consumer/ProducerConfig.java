package com.github.zzt93.syncer.config.consumer;

import com.github.zzt93.syncer.config.producer.ProducerInput;
import com.github.zzt93.syncer.config.producer.ProducerMaster;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author zzt
 */
@Slf4j
public class ProducerConfig {

  private String version;

  private Set<ProducerMaster> inputSet = new HashSet<>();

  public ProducerInput getInput() {
    return new ProducerInput(inputSet);
  }

  public void setInput(List<ProducerMaster> masters) {
    inputSet.addAll(masters);
    if (inputSet.size() < masters.size()) {
      log.warn("Duplicate mysql master connection endpoint");
    }
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }
}
