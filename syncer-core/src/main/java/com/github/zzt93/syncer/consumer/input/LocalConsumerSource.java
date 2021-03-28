package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncInitMeta;
import com.github.zzt93.syncer.config.common.Connection;
import com.github.zzt93.syncer.config.consumer.input.Repo;
import com.github.zzt93.syncer.consumer.ConsumerSource;
import com.github.zzt93.syncer.consumer.ack.Ack;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

/**
 * @author zzt
 */
public abstract class LocalConsumerSource implements ConsumerSource {

  private static final Logger logger = LoggerFactory.getLogger(LocalConsumerSource.class);
  private final BlockingQueue<SyncData> toFilter;
  private final Set<Repo> repos;
  private final Connection connection;
  private final SyncInitMeta syncInitMeta;
  private final String clientId;
  private final String connectionIdentifier;
  private final Ack ack;
  private boolean isSent = true;

  public LocalConsumerSource(
      String clientId, Connection connection, Set<Repo> repos,
      SyncInitMeta syncInitMeta,
      Ack ack, BlockingQueue<SyncData> toFilter) {
    this.repos = repos;
    this.connection = connection;
    this.syncInitMeta = syncInitMeta;
    this.clientId = clientId;
    this.ack = ack;
    this.toFilter = toFilter;
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

  @SneakyThrows
  @Override
  public boolean input(SyncData data) {
    if (sent(data)) {
      logger.info("Consumer({}, {}) skip {} from {}", getSyncInitMeta(), clientId, data, connectionIdentifier);
      return false;
    }
    ack.append(connectionIdentifier, data.getDataId());
    logger.debug("Consumer({}, {}) receive: {}", getSyncInitMeta(), clientId, data);
    toFilter.put(data.setSourceIdentifier(connectionIdentifier));
    return true;
  }

  @SneakyThrows
  @Override
  public boolean input(SyncData[] data) {
    for (SyncData datum : data) {
      if (datum == null) {
        continue;
      }
      input(datum);
    }
    return true;
  }

  @Override
  public boolean input(Collection<SyncData> data) {
    for (SyncData datum : data) {
      if (datum == null) {
        continue;
      }
      input(datum);
    }
    return true;
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
    return isSent = getSyncInitMeta().compareTo(data.getDataId().getSyncInitMeta()) > 0;
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
