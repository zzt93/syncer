package com.github.zzt93.syncer.input.connect;

/**
 * @author zzt
 */
public class BinlogInfo {

  private final String binlogFilename;
  private final long binlogPosition;

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
}
