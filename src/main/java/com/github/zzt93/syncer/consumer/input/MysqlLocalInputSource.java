package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.pipeline.common.Connection;
import com.github.zzt93.syncer.config.pipeline.input.Schema;
import com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo;
import java.util.Set;
import java.util.concurrent.BlockingDeque;

/**
 * @author zzt
 */
public class MysqlLocalInputSource extends LocalInputSource implements MysqlInputSource {

  private final BinlogInfo syncInitMeta;

  public MysqlLocalInputSource(String clientId,
      Connection connection,
      Set<Schema> schemas,
      BinlogInfo syncInitMeta,
      BlockingDeque<SyncData> input) {
    super(clientId, connection, schemas, syncInitMeta, input);
    this.syncInitMeta = syncInitMeta;
  }

  @Override
  public BinlogInfo getSyncInitMeta() {
    return syncInitMeta;
  }
}
