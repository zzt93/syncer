package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.config.pipeline.common.Connection;
import com.github.zzt93.syncer.config.pipeline.input.Repo;
import com.github.zzt93.syncer.producer.input.mongo.DocTimestamp;

import java.util.Set;

/**
 * @author zzt
 */
public class MongoLocalConsumerSource extends LocalConsumerSource implements MongoInputSource {

  private final DocTimestamp syncInitMeta;

  public MongoLocalConsumerSource(
      String clientId, Connection connection, Set<Repo> repos,
      DocTimestamp syncInitMeta,
      EventScheduler input) {
    super(clientId,connection, repos,syncInitMeta,input);
    this.syncInitMeta = syncInitMeta;
  }

  @Override
  public DocTimestamp getSyncInitMeta() {
    return syncInitMeta;
  }

}
