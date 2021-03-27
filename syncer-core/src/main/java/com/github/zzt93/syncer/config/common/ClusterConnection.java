package com.github.zzt93.syncer.config.common;

import com.github.zzt93.syncer.config.ConsumerConfig;
import com.github.zzt93.syncer.config.ConsumerInputConfig;
import com.github.zzt93.syncer.config.consumer.input.SyncMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
public class ClusterConnection extends Connection {

  static final String COLON = ":";
  private static final Logger logger = LoggerFactory.getLogger(ClusterConnection.class);
  @ConsumerConfig
  private String clusterName;
  @ConsumerConfig
  private List<String> clusterNodes;
  private HashSet<String> clusterIds;
  @ConsumerInputConfig
  private SyncMeta[] syncMetas;

  ClusterConnection() {
  }

  ClusterConnection(String clusterName, List<String> clusterNodes, String user, String passwordFile, String password, SyncMeta[] syncMetas) {
    this.clusterName = clusterName;
    this.clusterNodes = clusterNodes;
    this.syncMetas = syncMetas;
    setUser(user);
    setPassword(password);
    setPasswordFile(passwordFile);
  }

  static boolean validCluster(List<String> clusterNodes) {
    return clusterNodes != null && !clusterNodes.isEmpty();
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
    if (syncMetas == null) {
      syncMetas = new SyncMeta[target];
    }
    return syncMetas;
  }

  public void setSyncMetas(SyncMeta[] syncMetas) {
    this.syncMetas = syncMetas;
  }

  @Override
  public boolean valid() {
    return validCluster(getClusterNodes());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    ClusterConnection that = (ClusterConnection) o;
    return isSetEquals(that);
  }

  private boolean isSetEquals(ClusterConnection that) {
    HashSet<String> f = getClusterIds();
    HashSet<String> s = that.getClusterIds();
    if (f.size() != s.size()) return false;
    for (String id : f) {
      if (!s.contains(id)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClusterIds());
  }

  @Override
  public String toString() {
    return "ClusterConnection{" +
        "clusterName='" + clusterName + '\'' +
        ", clusterNodes=" + clusterNodes +
        ", clusterIds=" + getClusterIds() +
        '}';
  }

  @Override
  public String connectionIdentifier() {
    return getClusterIds().toString();
  }

  public List<Connection> getReals() {
    List<Connection> res = new ArrayList<>();
    for (String clusterNode : getClusterNodes()) {
      res.add(getConnection(clusterNode));
    }
    return res;
  }

  private Connection getConnection(String clusterNode) {
    Connection connection = new Connection(this);
    String[] split = clusterNode.split(COLON);
    if (split.length != 2) throw new InvalidConfigException("Invalid clusterNode: " + clusterNode + ". Not `addr:port` format.");
    connection.setPort(Integer.parseUnsignedInt(split[1]));
    try {
      connection.setAddress(split[0]);
    } catch (UnknownHostException e) {
      logger.error("Unknown host", e);
      throw new InvalidConfigException(e);
    }
    return connection;
  }

  public Set<String> remoteIds() {
    return getClusterIds();
  }

  public String getClusterName() {
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
}
