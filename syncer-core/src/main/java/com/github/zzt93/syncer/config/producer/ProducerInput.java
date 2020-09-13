package com.github.zzt93.syncer.config.producer;


import com.github.zzt93.syncer.config.ProducerConfig;
import lombok.Getter;

import java.util.Set;

/**
 * @author zzt
 */
@ProducerConfig("input")
@Getter
public class ProducerInput {

  private final Set<ProducerMaster> masterSet;


  public ProducerInput(Set<ProducerMaster> masterSet) {
    this.masterSet = masterSet;
  }
}
