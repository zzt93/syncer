package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncInitMeta;
import com.github.zzt93.syncer.config.pipeline.common.Connection;
import com.github.zzt93.syncer.config.pipeline.common.MasterSource;
import com.github.zzt93.syncer.config.pipeline.input.Schema;
import com.github.zzt93.syncer.consumer.InputSource;
import com.github.zzt93.syncer.producer.input.mongo.DocTimestamp;
import com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo;
import com.google.common.base.Preconditions;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public abstract class LocalInputSource implements InputSource {

  private final EventScheduler scheduler;
  private final Logger logger = LoggerFactory.getLogger(LocalInputSource.class);

  private final Set<Schema> schemas;
  private final Connection connection;
  private final SyncInitMeta syncInitMeta;
  private final String clientId;

  public LocalInputSource(
      String clientId, Connection connection, Set<Schema> schemas,
      SyncInitMeta syncInitMeta,
      EventScheduler scheduler) {
    this.schemas = schemas;
    this.connection = connection;
    this.syncInitMeta = syncInitMeta;
    this.clientId = clientId;
    this.scheduler = scheduler;
  }

  @Override
  public Connection getRemoteConnection() {
    return connection;
  }

  @Override
  public abstract SyncInitMeta getSyncInitMeta();

  @Override
  public Set<Schema> getSchemas() {
    return schemas;
  }

  @Override
  public String clientId() {
    return clientId;
  }

  @Override
  public boolean input(SyncData data) {
    logger.debug("add single: data id: {}, {}, {}", data.getDataId(), data, data.hashCode());
    data.setSourceIdentifier(connection.connectionIdentifier());
    return scheduler.schedule(data);
  }

  @Override
  public boolean input(SyncData[] data) {
    boolean res = true;
    for (SyncData datum : data) {
      res = scheduler.schedule(datum.setSourceIdentifier(connection.connectionIdentifier())) && res;
      logger.debug("add list: data id: {}, {}, {} in {}", datum.getDataId(), datum, datum.hashCode(),
          data);
    }
    return res;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LocalInputSource that = (LocalInputSource) o;

    return clientId.equals(that.clientId);
  }

  @Override
  public int hashCode() {
    return clientId.hashCode();
  }

  @Override
  public String toString() {
    return "LocalInputSource{" +
        "schemas=" + schemas +
        ", connection=" + connection +
        ", syncInitMeta=" + syncInitMeta +
        ", clientId='" + clientId + '\'' +
        '}';
  }

  public static LocalInputSource inputSource(String consumerId, MasterSource masterSource,
      SyncInitMeta syncInitMeta, EventScheduler scheduler) {
    LocalInputSource inputSource;
    switch (masterSource.getType()) {
      case Mongo:
        Preconditions
            .checkState(syncInitMeta instanceof DocTimestamp, "syncInitMeta is " + syncInitMeta);
        inputSource = new MongoLocalInputSource(consumerId, masterSource.getConnection(),
            masterSource.getSchemaSet(), (DocTimestamp) syncInitMeta, scheduler);
        break;
      case MySQL:
        Preconditions
            .checkState(syncInitMeta instanceof BinlogInfo, "syncInitMeta is " + syncInitMeta);
        inputSource = new MysqlLocalInputSource(consumerId, masterSource.getConnection(),
            masterSource.getSchemaSet(), (BinlogInfo) syncInitMeta, scheduler);
        break;
      default:
        throw new IllegalStateException("Not implemented type");
    }
    return inputSource;
  }
}
