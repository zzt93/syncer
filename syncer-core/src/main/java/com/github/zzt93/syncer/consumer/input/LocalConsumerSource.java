package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.common.IdGenerator;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncInitMeta;
import com.github.zzt93.syncer.config.pipeline.common.Connection;
import com.github.zzt93.syncer.config.pipeline.common.MasterSource;
import com.github.zzt93.syncer.config.pipeline.input.Repo;
import com.github.zzt93.syncer.consumer.ConsumerSource;
import com.github.zzt93.syncer.producer.input.mongo.DocTimestamp;
import com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * @author zzt
 */
public abstract class LocalConsumerSource implements ConsumerSource {

  private final EventScheduler scheduler;
  private final Logger logger = LoggerFactory.getLogger(LocalConsumerSource.class);

  private final Set<Repo> repos;
  private final Connection connection;
  private final SyncInitMeta syncInitMeta;
  private final String clientId;
  private boolean isSent = true;

  public LocalConsumerSource(
      String clientId, Connection connection, Set<Repo> repos,
      SyncInitMeta syncInitMeta,
      EventScheduler scheduler) {
    this.repos = repos;
    this.connection = connection;
    this.syncInitMeta = syncInitMeta;
    this.clientId = clientId;
    this.scheduler = scheduler;
  }

  public static LocalConsumerSource inputSource(String consumerId, MasterSource masterSource,
      SyncInitMeta syncInitMeta, EventScheduler scheduler) {
    LocalConsumerSource inputSource;
    switch (masterSource.getType()) {
      case Mongo:
        Preconditions
            .checkState(syncInitMeta instanceof DocTimestamp, "syncInitMeta is " + syncInitMeta);
        inputSource = new MongoLocalConsumerSource(consumerId, masterSource.getConnection(),
            masterSource.getRepoSet(), (DocTimestamp) syncInitMeta, scheduler);
        break;
      case MySQL:
        Preconditions
            .checkState(syncInitMeta instanceof BinlogInfo, "syncInitMeta is " + syncInitMeta);
        inputSource = new MysqlLocalConsumerSource(consumerId, masterSource.getConnection(),
            masterSource.getRepoSet(), (BinlogInfo) syncInitMeta, scheduler);
        break;
      default:
        throw new IllegalStateException("Not implemented type");
    }
    return inputSource;
  }

  @Override
  public Connection getRemoteConnection() {
    return connection;
  }

  @Override
  public abstract SyncInitMeta getSyncInitMeta();

  @Override
  public Set<Repo> getRepos() {
    return repos;
  }

  @Override
  public String clientId() {
    return clientId;
  }

  @Override
  public boolean input(SyncData data) {
    if (sent(data)) {
      logger.info("Consumer({}, {}) skip {} from {}", getSyncInitMeta(), clientId, data,
          connection.connectionIdentifier());
      return false;
    }
    logger.debug("add single: data id: {}, {}, {}", data.getDataId(), data, data.hashCode());
    data.setSourceIdentifier(connection.connectionIdentifier());
    return scheduler.schedule(data);
  }

  @Override
  public boolean input(SyncData[] data) {
    boolean res = true;
    for (SyncData datum : data) {
      if (!sent(datum)) {
        res = scheduler.schedule(datum.setSourceIdentifier(connection.connectionIdentifier())) && res;
        logger.debug("add list: data id: {}, {}, {} in {}", datum.getDataId(), datum,
            datum.hashCode(),
            data);
      } else {
        logger.info("Consumer({}, {}) skip {} from {}", getSyncInitMeta(), clientId, datum,
            connection.connectionIdentifier());
      }
    }
    return res;
  }

  /**
   * Because {@link #input(SyncData)} is called only by one thread, so we use {@link #isSent} as a
   * simple boolean
   */
  @Override
  public boolean sent(SyncData data) {
    if (!isSent) {
      return false;
    }

    // remembered position is not synced in last run,
    // if syncInitMeta.compareTo(now) == 0, is not sent
    return isSent = getSyncInitMeta().compareTo(IdGenerator.getSyncMeta(data.getDataId())) > 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LocalConsumerSource that = (LocalConsumerSource) o;

    return clientId.equals(that.clientId);
  }

  @Override
  public int hashCode() {
    return clientId.hashCode();
  }

  @Override
  public String toString() {
    return "LocalConsumerSource{" +
        "repos=" + repos +
        ", connection=" + connection +
        ", syncInitMeta=" + syncInitMeta +
        ", clientId='" + clientId + '\'' +
        '}';
  }
}
