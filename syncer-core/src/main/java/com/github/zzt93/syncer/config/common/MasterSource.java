package com.github.zzt93.syncer.config.common;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncInitMeta;
import com.github.zzt93.syncer.config.ConsumerConfig;
import com.github.zzt93.syncer.config.consumer.input.AutoOffsetReset;
import com.github.zzt93.syncer.config.consumer.input.MasterSourceType;
import com.github.zzt93.syncer.config.consumer.input.Repo;
import com.github.zzt93.syncer.config.consumer.input.SyncMeta;
import com.github.zzt93.syncer.consumer.ConsumerSource;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.input.LocalConsumerSource;
import com.github.zzt93.syncer.consumer.input.MongoLocalConsumerSource;
import com.github.zzt93.syncer.consumer.input.MysqlLocalConsumerSource;
import com.github.zzt93.syncer.producer.input.mongo.DocTimestamp;
import com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

/**
 * @author zzt
 */
@ConsumerConfig("input[]")
public class MasterSource {

  private final Logger logger = LoggerFactory.getLogger(MasterSource.class);
  private final Set<Repo> repoSet = new HashSet<>();
  private MasterSourceType type = MasterSourceType.MySQL;
  private MayClusterConnection connection;
  private List<Repo> repos = new ArrayList<>();

  private Connection getRealConnection() {
    return connection.getRealConnection();
  }

  public MayClusterConnection getConnection() {
    return connection;
  }

  public void setConnection(MayClusterConnection connection) {
    this.connection = connection;
  }

  public List<Repo> getRepos() {
    return repos;
  }

  public void setRepos(List<Repo> repos) {
    this.repos = repos;
    repoSet.addAll(repos);
    if (repoSet.size() < repos.size()) {
      logger.error("Duplicate repos: {}", repos);
      throw new InvalidConfigException("Duplicate repos");
    }
  }

  public Set<Repo> getRepoSet() {
    return repoSet;
  }

  public MasterSourceType getType() {
    return type;
  }

  public void setType(MasterSourceType type) {
    this.type = type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MasterSource that = (MasterSource) o;

    return getRealConnection().equals(that.getRealConnection());
  }

  @Override
  public int hashCode() {
    return getRealConnection().hashCode();
  }

  @Override
  public String toString() {
    return "MasterSource{" +
        "connection=" + connection +
        ", repos=" + repos +
        ", type=" + type +
        '}';
  }

  public Set<String> remoteIds() {
    return getRealConnection().remoteIds();
  }

  public List<? extends ConsumerSource> toConsumerSources(String consumerId,
                                                          Ack ack, HashMap<String, SyncInitMeta> ackConnectionId2SyncInitMeta,
                                                          BlockingQueue<SyncData> toFilter) {
    List<LocalConsumerSource> res = new LinkedList<>();
    Connection realConnection = getRealConnection();
    AutoOffsetReset autoOffsetReset = connection.getAutoOffsetReset();
    SyncMeta[] configSyncMetas = realConnection.getSyncMetas();
    List<Connection> connections = realConnection.getReals();
    for (int i = 0; i < connections.size(); i++) {
      Connection connection = connections.get(i);
      SyncInitMeta syncInitMeta = getSyncInitMeta(consumerId, configSyncMetas[i], ackConnectionId2SyncInitMeta.get(connection.connectionIdentifier()), autoOffsetReset);
      switch (getType()) {
        case Mongo:
          Preconditions
              .checkState(syncInitMeta instanceof DocTimestamp, "syncInitMeta is " + syncInitMeta);
          res.add(new MongoLocalConsumerSource(consumerId, connection,
              getRepoSet(), (DocTimestamp) syncInitMeta, ack, toFilter));
          break;
        case MySQL:
          Preconditions
              .checkState(syncInitMeta instanceof BinlogInfo, "syncInitMeta is " + syncInitMeta);
          res.add(new MysqlLocalConsumerSource(consumerId, connection,
              getRepoSet(), (BinlogInfo) syncInitMeta, ack, toFilter));
          break;
        default:
          throw new IllegalStateException("Not implemented type");
      }
    }
    return res;
  }

  private SyncInitMeta getSyncInitMeta(String consumerId, SyncMeta configSyncMeta, SyncInitMeta ackSyncMeta, AutoOffsetReset autoOffsetReset) {
    if (configSyncMeta != null) {
      logger.warn("Override [{}] remembered position with: {}", consumerId, configSyncMeta);
      return BinlogInfo.withFilenameCheck(configSyncMeta.getBinlogFilename(), configSyncMeta.getBinlogPosition());
    }
    if (autoOffsetReset != null) {
      logger.warn("Override [{}] remembered position with autoOffsetReset: {}", consumerId, autoOffsetReset);
      switch (autoOffsetReset) {
        case latest:
          return SyncInitMeta.latest(getType());
        case earliest:
          return SyncInitMeta.earliest(getType());
        default:
          throw new IllegalStateException();
      }
    }
    if (ackSyncMeta == null) {
      logger.info("[{}] to earliest possible position because no config and no last run info", consumerId);
      ackSyncMeta = SyncInitMeta.earliest(getType());
    }
    return ackSyncMeta;
  }
}
