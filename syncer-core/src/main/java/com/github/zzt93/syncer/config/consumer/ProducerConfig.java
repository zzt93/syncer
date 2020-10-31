package com.github.zzt93.syncer.config.consumer;

import com.github.zzt93.syncer.config.producer.ProducerInput;
import com.github.zzt93.syncer.config.producer.ProducerMaster;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.List;

/**
 * @author zzt
 */
@Slf4j
public class ProducerConfig {

  public String version;
  public List<ProducerMaster> input;

  public ProducerInput getInput() {
    HashSet<ProducerMaster> inputSet = new HashSet<>(input);
    if (inputSet.size() < input.size()) {
      log.warn("Duplicate mysql master connection endpoint");
    }
    return new ProducerInput(inputSet);
  }

}
