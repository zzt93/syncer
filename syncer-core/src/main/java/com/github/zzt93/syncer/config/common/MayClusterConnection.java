package com.github.zzt93.syncer.config.common;

import com.github.zzt93.syncer.config.consumer.input.SyncMeta;

import java.util.List;

/**
 * @author zzt
 */
public class MayClusterConnection {

  /**
   * @see ClusterConnection
   */
  private String clusterName;
  private List<String> clusterNodes;
  /**
   * @see Connection
   */
  private SyncMeta[] syncMetas;
  private String address;
  private int port;
  private String user;
  private String passwordFile;
  private String password;

  private Connection realConnection;

  private void build() {
    boolean cluster = ClusterConnection.validCluster(clusterNodes);
    boolean con = Connection.validConnection(address, port);
    if (cluster == con) {
      throw new InvalidConfigException("");
    }
    if (cluster) {
      realConnection = new ClusterConnection(clusterName, clusterNodes, syncMetas);
    } else {
      realConnection = new Connection(address, port, user, passwordFile, password, syncMetas == null ? null : syncMetas[0]);
    }
  }

  public Connection getRealConnection() {
    if (realConnection == null) {
      build();
    }
    return realConnection;
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

  public SyncMeta[] getSyncMetas() {
    return syncMetas;
  }

  public void setSyncMetas(SyncMeta[] syncMetas) {
    this.syncMetas = syncMetas;
  }

  public String getAddress() {
    return address;
  }

  public void setAddress(String address) {
    this.address = address;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPasswordFile() {
    return passwordFile;
  }

  public void setPasswordFile(String passwordFile) {
    this.passwordFile = passwordFile;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }


}
