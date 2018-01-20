package com.github.zzt93.syncer.producer.input.mysql.connect;

/**
 * @author zzt
 */
public class InvalidBinlogException extends IllegalStateException{

  private String binlogFilename;
  private long binlogPosition;

  public InvalidBinlogException(Throwable cause, String binlogFilename,
      long binlogPosition) {
    super(cause);
    this.binlogFilename = binlogFilename;
    this.binlogPosition = binlogPosition;
  }

  @Override
  public String toString() {
    return "InvalidBinlogException{" +
        "binlogFilename='" + binlogFilename + '\'' +
        ", binlogPosition=" + binlogPosition +
        '}';
  }
}
