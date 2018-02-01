package com.github.zzt93.syncer.common.data;

import com.github.zzt93.syncer.consumer.ack.Retryable;

/**
 * @author zzt
 */
public class SyncWrapper<T> implements Retryable {

  private final String syncDataId;
  private final String sourceId;
  private final T data;
  private final SyncData event;
  private int retryCount = 0;

  public SyncWrapper(SyncData event, T data) {
    this.syncDataId = event.getDataId();
    this.sourceId = event.getSourceIdentifier();
    this.data = data;
    this.event = event;
  }

  public String getSourceId() {
    return sourceId;
  }

  public String getSyncDataId() {
    return syncDataId;
  }

  public T getData() {
    return data;
  }

  public SyncData getEvent() {
    return event;
  }

  @Override
  public void inc() {
    retryCount++;
  }

  @Override
  public int retryCount() {
    return retryCount;
  }

  @Override
  public String toString() {
    return "SyncWrapper{" +
        "sourceId='" + sourceId + '\'' +
        ", syncDataId='" + syncDataId + '\'' +
        ", retryCount=" + retryCount +
        ", data=" + data +
        '}';
  }
}
