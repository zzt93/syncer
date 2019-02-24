package com.github.zzt93.syncer.config.common;

import com.github.zzt93.syncer.config.consumer.input.SyncMeta;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
@Getter
@Setter
@NoArgsConstructor
public class ClusterConnection extends Connection {

  static final String COLON = ":";
  private String clusterName;
  private List<String> clusterNodes;
  private HashSet<String> clusterIds;

  ClusterConnection(String clusterName, List<String> clusterNodes) {
    this.clusterName = clusterName;
    this.clusterNodes = clusterNodes;
  }

  private HashSet<String> getClusterIds() {
    if (clusterIds == null) {
      clusterIds = new HashSet<>(clusterNodes.size());
      for (String clusterNode : clusterNodes) {
        clusterIds.add(getConnection(clusterNode).connectionIdentifier());
      }
    }
    return clusterIds;
  }

  public SyncMeta[] getSyncMetas() {
    int target = getClusterNodes().size();
    SyncMeta[] syncMetas = super.getSyncMetas();
    if (syncMetas == null) {
      syncMetas = new SyncMeta[target];
    } else if (syncMetas.length != target) {
      throw new InvalidConfigException("syncMetas.size() != clusterNodes.size()");
    }
    return syncMetas;
  }

  @Override
  public boolean valid() {
    return validCluster(getClusterNodes());
  }

  static boolean validCluster(List<String> clusterNodes) {
    return clusterNodes != null && !clusterNodes.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    ClusterConnection that = (ClusterConnection) o;
    return Objects.equals(clusterIds, that.clusterIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), clusterIds);
  }

  @Override
  public String toString() {
    return "ClusterConnection{" +
        "clusterName='" + clusterName + '\'' +
        ", clusterNodes=" + clusterNodes +
        ", clusterIds=" + clusterIds +
        '}';
  }

  @Override
  public String connectionIdentifier() {
    return getClusterIds().toString();
  }

  public List<Connection> getConnections() {
    List<Connection> res = new ArrayList<>();
    for (String clusterNode : getClusterNodes()) {
      res.add(getConnection(clusterNode));
    }
    return res;
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

  public Set<String> remoteIds() {
    return getClusterIds();
  }

}
