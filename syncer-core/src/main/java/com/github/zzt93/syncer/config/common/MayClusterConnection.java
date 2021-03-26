package com.github.zzt93.syncer.config.common;

import com.github.zzt93.syncer.config.ConsumerConfig;
import com.github.zzt93.syncer.config.ProducerConfig;
import com.github.zzt93.syncer.config.consumer.input.AutoOffsetReset;
import com.github.zzt93.syncer.config.consumer.input.MasterSourceType;
import com.github.zzt93.syncer.config.consumer.input.SyncMeta;
import lombok.Data;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.List;

/**
 * @author zzt
 */
@Data
@ProducerConfig("input[].connection")
@ConsumerConfig("input[].connection")
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

  /**
   * What to do when there is no initial offset in Syncer or if the current offset no
   * longer exists on the server.
   */
  private AutoOffsetReset autoOffsetReset;

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

  public void validate(MasterSourceType type) {
    boolean empty = StringUtils.isEmpty(password);
    boolean emptyFile = StringUtils.isEmpty(passwordFile);
    if (!empty && !emptyFile) {
      throw new InvalidConfigException("Should not config both `password` and `passwordFile`");
    }
    boolean emptyUser = StringUtils.isEmpty(user);
    if (((empty && emptyFile) || emptyUser)) {
      if (type == MasterSourceType.MySQL) {
        throw new InvalidConfigException("Lack `user` or `password` for MySQL");
      } else if (emptyUser != (empty && emptyFile)) {
        throw new InvalidConfigException("Lack `user` or `password` for Mongo");
      }
    }
  }
}
