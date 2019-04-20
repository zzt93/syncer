package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.common.IdGenerator;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncInitMeta;
import com.github.zzt93.syncer.config.common.Connection;
import com.github.zzt93.syncer.config.consumer.input.Repo;
import com.github.zzt93.syncer.consumer.ConsumerSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * @author zzt
 */
public abstract class LocalConsumerSource implements ConsumerSource {

  private static final Logger logger = LoggerFactory.getLogger(LocalConsumerSource.class);
  private final EventScheduler scheduler;
  private final Set<Repo> repos;
  private final Connection connection;
  private final SyncInitMeta syncInitMeta;
  private final String clientId;
  private final String connectionIdentifier;
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
    connectionIdentifier = connection.connectionIdentifier();
  }

  @Override
  public Connection getRemoteConnection() {
    return connection;
  }

  @Override
  public abstract SyncInitMeta getSyncInitMeta();

  @Override
  public Set<Repo> copyRepos() {
    Set<Repo> res = new HashSet<>();
    for (Repo repo : repos) {
      res.add(new Repo(repo));
    }
    return res;
  }

  @Override
  public String clientId() {
    return clientId;
  }

  @Override
  public boolean input(SyncData data) {
    if (sent(data)) {
      logger.info("Consumer({}, {}) skip {} from {}", getSyncInitMeta(), clientId, data, connectionIdentifier);
      return false;
    }
    logger.debug("add single: data id: {}, {}, {}", data.getDataId(), data, data.hashCode());
    data.setSourceIdentifier(connectionIdentifier);
    return scheduler.schedule(data);
  }

  @Override
  public boolean input(SyncData[] data) {
    boolean res = true;
    for (SyncData datum : data) {
      if (!sent(datum)) {
        res = scheduler.schedule(datum.setSourceIdentifier(connectionIdentifier)) && res;
        logger.debug("Consumer receive: {} in {}", datum, data);
      } else {
        logger.info("Consumer({}, {}) skip {} from {}", getSyncInitMeta(), clientId, datum,
            connectionIdentifier);
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
