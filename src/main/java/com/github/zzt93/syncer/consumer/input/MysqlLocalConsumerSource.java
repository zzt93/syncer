package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.config.pipeline.common.Connection;
import com.github.zzt93.syncer.config.pipeline.input.Schema;
import com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo;

import java.util.Set;

/**
 * @author zzt
 */
public class MysqlLocalConsumerSource extends LocalConsumerSource implements MysqlInputSource {

  private final BinlogInfo syncInitMeta;

  public MysqlLocalConsumerSource(String clientId,
                                  Connection connection,
                                  Set<Schema> schemas,
                                  BinlogInfo syncInitMeta,
                                  EventScheduler input) {
    super(clientId, connection, schemas, syncInitMeta, input);
    this.syncInitMeta = syncInitMeta;
  }

  @Override
  public BinlogInfo getSyncInitMeta() {
    return syncInitMeta;
  }
}
