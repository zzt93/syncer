package com.github.zzt93.syncer.config.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
public class ClusterConnection extends Connection {

  static final String COMMA = ",";
  static final String COLON = ":";
  private final Logger logger = LoggerFactory.getLogger(ClusterConnection.class);
  private String clusterName;
  private List<String> clusterNodes;


  String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public List<String> getClusterNodes() {
    return clusterNodes;
  }

  public void setClusterNodes(List<String> clusterNodes) {
    if (clusterNodes.isEmpty()) {
      throw new InvalidConfigException("clusterNodes is empty");
    }
    this.clusterNodes = clusterNodes;
  }

  @Override
  public boolean valid() {
    return clusterNodes != null && !clusterNodes.isEmpty();
  }

  @Override
  public String connectionIdentifier() {
    return clusterNodes.get(0);
  }
}
