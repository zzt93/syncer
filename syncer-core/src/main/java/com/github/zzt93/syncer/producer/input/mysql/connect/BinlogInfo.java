package com.github.zzt93.syncer.producer.input.mysql.connect;

import com.github.zzt93.syncer.common.data.SyncInitMeta;
import com.github.zzt93.syncer.config.common.Connection;
import com.github.zzt93.syncer.consumer.ConsumerSource;
import com.github.zzt93.syncer.producer.register.LocalConsumerRegistry;

/**
 * @author zzt
 */
public class BinlogInfo implements SyncInitMeta<BinlogInfo> {

  private final String binlogFilename;
  private final long binlogPosition;

  public static final BinlogInfo latest = new BinlogInfo(null, 0);
  public static final BinlogInfo earliest = new BinlogInfo("", 0);

  public BinlogInfo(String binlogFilename, long binlogPosition) {
    this.binlogFilename = binlogFilename;
    this.binlogPosition = binlogPosition;
  }

  public static BinlogInfo withFilenameCheck(String binlogFilename, long binlogPosition) {
    checkFilename(binlogFilename);
    return new BinlogInfo(binlogFilename, binlogPosition);
  }

  static int checkFilename(String binlogFilename) {
    try {
      return Integer.parseInt(binlogFilename.split("\\.")[1]);
    } catch (NumberFormatException|ArrayIndexOutOfBoundsException e) {
      throw new InvalidBinlogException(e, binlogFilename, 0);
    }
  }

  public String getBinlogFilename() {
    return binlogFilename;
  }

  public long getBinlogPosition() {
    return binlogPosition;
  }

  /**
   * <ul>
   *   <li>If some consumer is {@link #earliest}, i.e. fresh start, try retrieve oldest log to sync.</li>
   *   <li>If some consumer is {@link #latest}, i.e. fresh start, try retrieve latest log to sync.</li>
   *   <li>If we need to sync even earlier log than oldest log possible, try to config {@link
   *   com.github.zzt93.syncer.config.producer.ProducerMaster#file}</li>
   *   <li><a href='https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#option_mysqld_log-bin'>
   *    Binary log naming conversion: base_name.number, e.g. mysql-bin.000042</a></li>
   * </ul>
   *
   * <p>We can't compare using string
   * because number suffix will inc, as <a href='https://dba.stackexchange.com/questions/94286/what-will-happen-to-the-binary-log-if-it-reaches-its-maximum-value-does-it-res'>here</a>
   * said</p>
   *
   * @see LocalConsumerRegistry#register(Connection, ConsumerSource)
   */
  @Override
  public int compareTo(BinlogInfo o) {
    if (o == this) {
      return 0;
    }

    if (o == earliest) {
      return -1;
    } else if (o == latest) {
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
