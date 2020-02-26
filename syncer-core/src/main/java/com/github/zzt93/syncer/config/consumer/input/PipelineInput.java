package com.github.zzt93.syncer.config.consumer.input;


import com.github.zzt93.syncer.config.ConsumerConfig;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.common.MasterSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author zzt
 */
@ConsumerConfig("input")
public class PipelineInput {

  private Logger logger = LoggerFactory.getLogger(PipelineInput.class);

  private List<MasterSource> masters = new ArrayList<>();
  private Set<MasterSource> masterSet = new HashSet<>();

  public List<MasterSource> getMasters() {
    return masters;
  }

  public void setMasters(List<MasterSource> masters) {
    this.masters = masters;
    masterSet.addAll(masters);
    if (masterSet.size() < masters.size()) {
      logger.error("Duplicate master source: {}", masters);
      throw new InvalidConfigException("Duplicate master source");
    }
  }

  public Set<MasterSource> getMasterSet() {
    return masterSet;
  }

}
