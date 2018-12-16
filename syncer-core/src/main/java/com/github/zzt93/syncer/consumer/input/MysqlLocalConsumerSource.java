package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.config.common.Connection;
import com.github.zzt93.syncer.config.consumer.input.Repo;
import com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo;

import java.util.Set;

/**
 * @author zzt
 */
public class MysqlLocalConsumerSource extends LocalConsumerSource implements MysqlInputSource {

  private final BinlogInfo syncInitMeta;

  public MysqlLocalConsumerSource(String clientId,
                                  Connection connection,
                                  Set<Repo> repos,
                                  BinlogInfo syncInitMeta,
                                  EventScheduler input) {
    super(clientId, connection, repos, syncInitMeta, input);
    this.syncInitMeta = syncInitMeta;
  }

  @Override
  public BinlogInfo getSyncInitMeta() {
    return syncInitMeta;
  }
}
