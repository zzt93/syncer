package com.github.zzt93.syncer.producer.input.mysql.connect;

import com.github.zzt93.syncer.common.data.SyncInitMeta;
import com.github.zzt93.syncer.config.pipeline.common.Connection;
import com.github.zzt93.syncer.consumer.ConsumerSource;
import com.github.zzt93.syncer.producer.register.LocalConsumerRegistry;

/**
 * @author zzt
 */
public class BinlogInfo implements SyncInitMeta<BinlogInfo> {

  private final String binlogFilename;
  private final long binlogPosition;

  public BinlogInfo() {
    binlogFilename = null;
    binlogPosition = 0;
  }

  public BinlogInfo(String binlogFilename, long binlogPosition) {
    this.binlogFilename = binlogFilename;
    this.binlogPosition = binlogPosition;
  }

  public static BinlogInfo withFilenameCheck(String binlogFilename, long binlogPosition) {
    // example: mysql-bin.000039
    checkFilename(binlogFilename);
    return new BinlogInfo(binlogFilename, binlogPosition);
  }

  private static void checkFilename(String binlogFilename) {
    try {
      Integer.parseInt(binlogFilename.split("\\.")[1]);
    } catch (NumberFormatException e) {
      throw new InvalidBinlogException(e, binlogFilename, 0);
    }
  }

  public String getBinlogFilename() {
    return binlogFilename;
  }

  public long getBinlogPosition() {
    return binlogPosition;
  }

  boolean isEmpty() {
    return binlogFilename == null || binlogFilename.equals("");
  }

  /**
   * If some consumer is {@link #isEmpty()}, i.e. fresh start,
   * try retrieve oldest log to sync.
   *
   * If need to sync even earlier log than oldest log possible,
   * try to config {@link com.github.zzt93.syncer.config.producer.ProducerMaster#file}
   *
   * @see LocalConsumerRegistry#register(Connection, ConsumerSource)
   * @see MysqlMasterConnector#oldestLog(InvalidBinlogException)
   */
  @Override
  public int compareTo(BinlogInfo o) {
    if (isEmpty() && o.isEmpty()) {
      return 0;
    } else if (isEmpty()) {
      return -1;
    } else if (o.isEmpty()) {
      return 1;
    }
    int seq = Integer.parseInt(binlogFilename.split("\\.")[1]);
    int oSeq = Integer.parseInt(o.binlogFilename.split("\\.")[1]);
    int compare = Integer.compare(seq, oSeq);
    return compare != 0 ? compare : Long.compare(binlogPosition, o.binlogPosition);
  }

  @Override
  public String toString() {
    return "BinlogInfo{" +
        "binlogFilename='" + binlogFilename + '\'' +
        ", binlogPosition=" + binlogPosition +
        '}';
  }
}
