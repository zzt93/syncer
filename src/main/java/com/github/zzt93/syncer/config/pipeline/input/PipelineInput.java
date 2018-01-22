package com.github.zzt93.syncer.config.pipeline.input;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
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
      logger.warn("Duplicate mysql master connection endpoint");
    }
  }

  public Set<MasterSource> getMasterSet() {
    return masterSet;
  }

}
