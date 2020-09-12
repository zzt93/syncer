package com.github.zzt93.syncer.stat.vo;

public class BatchBufferStat<T> {

  private String outputChannelId;
  private int limit;
  private int now;
  private T firstKey;
  private T lastKey;

  public BatchBufferStat(String outputChannelId, int limit, int now, T firstKey, T lastKey) {
    this.outputChannelId = outputChannelId;
    this.limit = limit;
    this.now = now;
    this.firstKey = firstKey;
    this.lastKey = lastKey;
  }

  @Override
  public String toString() {
    return "BatchBufferStat{" +
        "outputChannelId='" + outputChannelId + '\'' +
        ", limit=" + limit +
        ", now=" + now +
        ", firstKey=" + firstKey +
        ", lastKey=" + lastKey +
        '}';
  }
}
