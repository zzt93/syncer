package com.github.zzt93.syncer.config.common;

import com.github.zzt93.syncer.config.consumer.input.SyncMeta;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

/**
 * @author zzt
 */
@Getter
@Setter
@ToString
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
      realConnection = new ClusterConnection(clusterName, clusterNodes);
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
}
