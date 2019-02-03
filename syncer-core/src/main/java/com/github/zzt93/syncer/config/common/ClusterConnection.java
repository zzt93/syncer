package com.github.zzt93.syncer.config.common;

import com.github.zzt93.syncer.config.consumer.input.SyncMeta;
import com.google.common.collect.Lists;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
public class ClusterConnection extends Connection {

  static final String COLON = ":";
  private String clusterName;
  private List<String> clusterNodes;
  private List<String> clusterIds;
  private SyncMeta[] syncMetas;

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

  private List<String> getClusterIds() {
    if (clusterIds == null) {
      clusterIds = new ArrayList<>(clusterNodes.size());
      for (String clusterNode : clusterNodes) {
        clusterIds.add(getConnection(clusterNode).connectionIdentifier());
      }
    }
    return clusterIds;
  }

  public SyncMeta[] getSyncMetas() {
    int target = super.valid() ? 1 : getClusterNodes().size();
    if (syncMetas == null) {
      syncMetas = new SyncMeta[target];
    } else if (syncMetas.length != target) {
      throw new InvalidConfigException("syncMetas.size() != clusterNodes.size()");
    }
    return syncMetas;
  }

  public void setSyncMetas(SyncMeta[] syncMetas) {
    this.syncMetas = syncMetas;
  }

  @Override
  public boolean valid() {
    boolean validConnection = super.valid();
    boolean validCluster = isValidCluster();
    return validCluster != validConnection;
  }

  private boolean isValidCluster() {
    return getClusterNodes() != null && !getClusterNodes().isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    ClusterConnection that = (ClusterConnection) o;
    return clusterOrSimple(() -> Objects.equals(getClusterIds(), that.getClusterIds()), () -> super.equals(o));
  }

  @Override
  public int hashCode() {
    return clusterOrSimple(() -> Objects.hash(getClusterIds()), super::hashCode);
  }

  @Override
  public String toString() {
    return "ClusterConnection{" +
        "super=" + super.toString() +
        ", clusterName='" + clusterName + '\'' +
        ", clusterNodes=" + getClusterNodes() +
        '}';
  }

  @Override
  public String connectionIdentifier() {
    return clusterOrSimple(() -> getClusterIds().toString(), super::connectionIdentifier);
  }

  List<Connection> getConnections() {
    return clusterOrSimple(() -> {
      List<Connection> res = new ArrayList<>();
      for (String clusterNode : getClusterNodes()) {
        Connection e = getConnection(clusterNode);
        res.add(e);
      }
      return res;
    }, () -> Lists.newArrayList(this));
  }

  private Connection getConnection(String clusterNode) {
    Connection e = new Connection(this);
    String[] split = clusterNode.split(COLON);
    if (split.length != 2) throw new InvalidConfigException(clusterNode);
    e.setPort(Integer.parseUnsignedInt(split[1]));
    try {
      e.setAddress(split[0]);
    } catch (UnknownHostException e1) {
      throw new InvalidConfigException(e1);
    }
    return e;
  }

  List<String> remoteIds() {
    return clusterOrSimple(
        this::getClusterIds,
        () -> Lists.newArrayList(super.connectionIdentifier())
    );
  }

  private <T> T clusterOrSimple(Supplier<T> cluster, Supplier<T> simple) {
    boolean valid = super.valid();
    boolean validCluster = isValidCluster();
    if (valid == validCluster) {
      throw new InvalidConfigException("Config both address and clusterNodes");
    } else if (valid) {
      return simple.get();
    } else {
      return cluster.get();
    }
  }

}
