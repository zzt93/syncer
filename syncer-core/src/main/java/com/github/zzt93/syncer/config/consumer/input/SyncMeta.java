package com.github.zzt93.syncer.config.consumer.input;

/**
 * Only for config mysql/drds `BinlogInfo` for the time being,
 * may support `DocTimestamp` in future
 *
 * @see com.github.zzt93.syncer.common.data.SyncInitMeta
 * @see com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo
 * @see com.github.zzt93.syncer.producer.input.mongo.DocTimestamp
 * @author zzt
 */
public class SyncMeta {

  private String binlogFilename;
  private long binlogPosition;

  public String getBinlogFilename() {
    return binlogFilename;
  }

  public void setBinlogFilename(String binlogFilename) {
    this.binlogFilename = binlogFilename;
  }

  public long getBinlogPosition() {
    return binlogPosition;
  }

  public void setBinlogPosition(long binlogPosition) {
    this.binlogPosition = binlogPosition;
  }

  @Override
  public String toString() {
    return "SyncMeta{" +
        "binlogFilename='" + binlogFilename + '\'' +
        ", binlogPosition=" + binlogPosition +
        '}';
  }
}
