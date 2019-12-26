package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.config.common.Connection;
import com.github.zzt93.syncer.config.consumer.input.Repo;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo;

import java.util.Set;

/**
 * @author zzt
 */
public class MysqlLocalConsumerSource extends LocalConsumerSource implements MysqlInputSource {

  private BinlogInfo syncInitMeta;

  public MysqlLocalConsumerSource(String clientId,
                                  Connection connection,
                                  Set<Repo> repos,
                                  BinlogInfo syncInitMeta,
                                  Ack ack, EventScheduler input) {
    super(clientId, connection, repos, syncInitMeta, ack, input);
    this.syncInitMeta = syncInitMeta;
  }

  @Override
  public BinlogInfo getSyncInitMeta() {
    return syncInitMeta;
  }

  @Override
  public void replaceLatest(BinlogInfo latest) {
    syncInitMeta = latest;
  }
}
