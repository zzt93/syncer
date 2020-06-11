package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.common.Connection;
import com.github.zzt93.syncer.config.consumer.input.Repo;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.producer.input.mongo.DocTimestamp;

import java.util.Set;
import java.util.concurrent.BlockingDeque;

/**
 * @author zzt
 */
public class MongoLocalConsumerSource extends LocalConsumerSource implements MongoInputSource {

  private final DocTimestamp syncInitMeta;

  public MongoLocalConsumerSource(
      String clientId, Connection connection, Set<Repo> repos,
      DocTimestamp syncInitMeta,
      Ack ack, BlockingDeque<SyncData> toFilter) {
    super(clientId, connection, repos, syncInitMeta, ack, toFilter);
    this.syncInitMeta = syncInitMeta;
  }

  @Override
  public DocTimestamp getSyncInitMeta() {
    return syncInitMeta;
  }

}
