package com.github.zzt93.syncer.common.data;

import com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo;

/**
 * @author zzt
 */
public class ColdStartDataId {

  /**
   * cold start not log in ack log:
   * <li>no suitable id</li>
   * <li>affect performance</li>
   *
   * @see #isCold(DataId)
   */
  public static final BinlogDataId BINLOG_COLD = new BinlogDataId("", 0L, 0L) {
    @Override
    public SyncInitMeta getSyncInitMeta() {
      return BinlogInfo.earliest;
    }

    @Override
    public BinlogDataId copyAndSetOrdinal(int ordinal) {
      return this;
    }

    @Override
    public BinlogDataId copyAndCount(int copy) {
      return this;
    }

    @Override
    public String toString() {
      return "cold-start";
    }
  };

  public static boolean isCold(DataId dataId) {
    return dataId == BINLOG_COLD;
  }
}
