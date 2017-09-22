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

  private List<MysqlMaster> mysqlMasters = new ArrayList<>();
  private Set<MysqlMaster> mysqlMasterSet = new HashSet<>();

  public List<MysqlMaster> getMysqlMasters() {
    return mysqlMasters;
  }

  public void setMysqlMasters(List<MysqlMaster> mysqlMasters) {
    this.mysqlMasters = mysqlMasters;
    mysqlMasterSet.addAll(mysqlMasters);
    if (mysqlMasterSet.size() < mysqlMasters.size()) {
      logger.warn("Duplicate mysql master connection endpoint");
    }
  }

  public Set<MysqlMaster> getMysqlMasterSet() {
    return mysqlMasterSet;
  }

}
