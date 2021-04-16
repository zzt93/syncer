package com.github.zzt93.syncer.common.data;

import com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo;

import java.util.Comparator;
import java.util.Objects;

/**
 * @author zzt
 */
public class BinlogDataId implements DataId {
  private static final int COMMON_LEN = 40;

  private final String binlogFileName;
  private final long tableMapPos;
  private final long dataPos;
  private int ordinal;
  private Integer copy;

  public BinlogDataId(String binlogFileName, long tableMapPos, long dataPos) {
    this.binlogFileName = binlogFileName;
    this.tableMapPos = tableMapPos;
    this.dataPos = dataPos;
  }

  public BinlogDataId setOrdinal(int ordinal) {
    this.ordinal = ordinal;
    return this;
  }

  public BinlogDataId setCopy(Integer copy) {
    this.copy = copy;
    return this;
  }

  public String eventId() {
    return new StringBuilder(COMMON_LEN)
        .append(binlogFileName).append(SEP)
        .append(tableMapPos).append(SEP)
        .append(dataPos - tableMapPos)
        .toString();
  }

  public String dataId() {
    StringBuilder append = new StringBuilder(COMMON_LEN).append(eventId()).append(SEP).append(ordinal);
    if (copy != null) {
      return append.append(SEP).append(copy).toString();
    } else {
      return append.toString();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BinlogDataId that = (BinlogDataId) o;
    return tableMapPos == that.tableMapPos &&
        dataPos == that.dataPos &&
        ordinal == that.ordinal &&
        binlogFileName.equals(that.binlogFileName) &&
        Objects.equals(copy, that.copy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(binlogFileName, tableMapPos, dataPos, ordinal, copy);
  }

  @Override
  public SyncInitMeta getSyncInitMeta() {
    return new BinlogInfo(binlogFileName, tableMapPos);
  }

  @Override
  public int compareTo(DataId dataId) {
    BinlogDataId o = (BinlogDataId) dataId;

    int cmp = getSyncInitMeta().compareTo(o.getSyncInitMeta());
    if (cmp != 0) {
      return cmp;
    }
    cmp = Long.compare(tableMapPos, o.tableMapPos);
    if (cmp != 0) {
      return cmp;
    }
    cmp = Long.compare(dataPos, o.dataPos);
    if (cmp != 0) {
      return cmp;
    }
    cmp = Integer.compare(ordinal, o.ordinal);
    if (cmp != 0) {
      return cmp;
    }
    return Objects.equals(copy, o.copy) ? 0 :
        copy == null ? -1 :
            o.copy == null ? 1 :
                Objects.compare(copy, o.copy, Comparator.naturalOrder());
  }

  public BinlogDataId copyAndSetOrdinal(int ordinal) {
    return new BinlogDataId(binlogFileName, tableMapPos, dataPos).setOrdinal(ordinal);
  }

  public BinlogDataId copyAndCount(int copy) {
    return new BinlogDataId(binlogFileName, tableMapPos, dataPos).setOrdinal(ordinal).setCopy(copy);
  }

  @Override
  public String toString() {
    return dataId();
  }

}
