package com.github.zzt93.syncer.producer.input.connect;

/**
 * @author zzt
 */
public class BinlogInfo implements Comparable<BinlogInfo> {

  private final String binlogFilename;
  private final long binlogPosition;

  public BinlogInfo() {
    // TODO 18/1/12 default/empty
    binlogFilename = "mysql-bin.0";
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
    int seq = Integer.parseInt(binlogFilename.split("\\.")[1]);
    int oSeq = Integer.parseInt(o.binlogFilename.split("\\.")[1]);
    int compare = Integer.compare(seq, oSeq);
    return compare != 0 ? compare : Long.compare(binlogPosition, binlogPosition);
  }
}
