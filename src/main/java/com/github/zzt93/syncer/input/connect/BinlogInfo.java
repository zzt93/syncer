package com.github.zzt93.syncer.input.connect;

/**
 * @author zzt
 */
public class BinlogInfo {

  private final String binlogFilename;
  private final long binlogPosition;

  BinlogInfo(String binlogFilename, long binlogPosition) {
    this.binlogFilename = binlogFilename;
    this.binlogPosition = binlogPosition;
  }

  String getBinlogFilename() {
    return binlogFilename;
  }

  long getBinlogPosition() {
    return binlogPosition;
  }
}
