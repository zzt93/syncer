package com.github.zzt93.syncer.config.common;

import com.github.zzt93.syncer.config.consumer.input.SyncMeta;
import org.springframework.util.StringUtils;

import java.util.Arrays;
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
  private String address;
  private int port;
  /**
   * shared
   */
  private SyncMeta[] syncMetas;
  private String user;
  private String passwordFile;
  private String password;

  private Connection realConnection;

  private void build() {
    boolean cluster = ClusterConnection.validCluster(clusterNodes);
    boolean con = Connection.validConnection(address, port);
    if (cluster == con) {
      throw new InvalidConfigException("Config one of `address & port` or `clusterNodes`");
    }
    if (cluster) {
      realConnection = new ClusterConnection(clusterName, clusterNodes, user, passwordFile, password, syncMetas);
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

  @Override
  public String toString() {
    return "MayClusterConnection{" +
        "clusterName='" + clusterName + '\'' +
        ", clusterNodes=" + clusterNodes +
        ", syncMetas=" + Arrays.toString(syncMetas) +
        ", address='" + address + '\'' +
        ", port=" + port +
        ", user='" + user + '\'' +
        ", passwordFile='" + passwordFile + '\'' +
        ", password='***'" +
        ", realConnection=" + realConnection +
        '}';
  }

  public void checkPassword() {
    boolean empty = StringUtils.isEmpty(password);
    if (empty == StringUtils.isEmpty(passwordFile) && !empty) {
      throw new InvalidConfigException("Should not config both `password` and `passwordFile`");
    }

  }
}
