package com.github.zzt93.syncer.producer.input.connect;

/**
 * @author zzt
 */
public class BinlogInfo implements Comparable<BinlogInfo> {

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

  public String getBinlogFilename() {
    return binlogFilename;
  }

  public long getBinlogPosition() {
    return binlogPosition;
  }

  boolean isEmpty() {
    return binlogFilename == null || binlogFilename.equals("");
  }

  @Override
  public int compareTo(BinlogInfo o) {
    if (binlogFilename == null && o.binlogFilename == null) {
      return 0;
    } else if (binlogFilename == null) {
      return -1;
    } else if (o.binlogFilename == null) {
      return 1;
    }
    int seq = Integer.parseInt(binlogFilename.split("\\.")[1]);
    int oSeq = Integer.parseInt(o.binlogFilename.split("\\.")[1]);
    int compare = Integer.compare(seq, oSeq);
    return compare != 0 ? compare : Long.compare(binlogPosition, binlogPosition);
  }

  @Override
  public String toString() {
    return "BinlogInfo{" +
        "binlogFilename='" + binlogFilename + '\'' +
        ", binlogPosition=" + binlogPosition +
        '}';
  }
}
