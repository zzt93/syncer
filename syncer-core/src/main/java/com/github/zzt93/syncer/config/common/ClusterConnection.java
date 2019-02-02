package com.github.zzt93.syncer.config.common;

import com.github.zzt93.syncer.config.consumer.input.SyncMeta;
import com.google.common.collect.Lists;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
public class ClusterConnection extends Connection {

  static final String COLON = ":";
  private String clusterName;
  private List<String> clusterNodes;
  private List<SyncMeta> syncMetas;

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
    this.clusterNodes = clusterNodes;
  }

  public List<SyncMeta> getSyncMetas() {
    int target = super.valid() ? 1 : clusterNodes.size();
    if (syncMetas == null) {
      syncMetas = new ArrayList<>(target);
    } else if (syncMetas.size() != target) {
      throw new InvalidConfigException("syncMetas.size() != clusterNodes.size()");
    }
    return syncMetas;
  }

  public void setSyncMetas(List<SyncMeta> syncMetas) {
    this.syncMetas = syncMetas;
  }

  @Override
  public boolean valid() {
    boolean validConnection = super.valid();
    boolean validCluster = isValidCluster();
    return validCluster != validConnection;
  }

  private boolean isValidCluster() {
    return clusterNodes != null && !clusterNodes.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    ClusterConnection that = (ClusterConnection) o;
    return Objects.equals(clusterNodes, that.clusterNodes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), clusterNodes);
  }

  @Override
  public String toString() {
    return "ClusterConnection{" +
        "super=" + super.toString() +
        ", clusterName='" + clusterName + '\'' +
        ", clusterNodes=" + clusterNodes +
        '}';
  }

  @Override
  public String connectionIdentifier() {
    return clusterNodes.get(0);
  }

  List<Connection> getConnections() {
    boolean valid = super.valid();
    boolean validCluster = isValidCluster();
    if (valid == validCluster) {
      throw new InvalidConfigException("Config both address and clusterNodes");
    } else if (valid) {
      return Lists.newArrayList(this);
    } else {
      // a cluster connection
      List<Connection> res = new ArrayList<>();
      for (String clusterNode : clusterNodes) {
        Connection e = new Connection(this);
        String[] split = clusterNode.split(COLON);
        if (split.length != 2) throw new InvalidConfigException(clusterNode);
        e.setPort(Integer.parseUnsignedInt(split[1]));
        try {
          e.setAddress(split[0]);
        } catch (UnknownHostException e1) {
          throw new InvalidConfigException(e1);
        }
        res.add(e);
      }
      return res;
    }
  }

  List<String> remoteIds() {
    if (isValidCluster()) {
      return clusterNodes;
    } else {
      return Lists.newArrayList(super.connectionIdentifier());
    }
  }
}
