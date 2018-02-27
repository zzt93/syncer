package com.github.zzt93.syncer.config.producer;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author zzt
 */
@ConfigurationProperties("syncer.producer.input")
public class ProducerInput {

  private Logger logger = LoggerFactory.getLogger(ProducerInput.class);

  private List<ProducerMaster> masters = new ArrayList<>();
  private Set<ProducerMaster> masterSet = new HashSet<>();

  @ConfigurationProperties("syncer.producer.input.masters")
  public List<ProducerMaster> getMasters() {
    return masters;
  }

  public void setMasters(List<ProducerMaster> masters) {
    this.masters = masters;
    masterSet.addAll(masters);
    if (masterSet.size() < masters.size()) {
      logger.warn("Duplicate mysql master connection endpoint");
    }
  }

  public Set<ProducerMaster> masterSet() {
    return masterSet;
  }

}
