package com.github.zzt93.syncer.config.pipeline.input;

/**
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
