package com.github.zzt93.syncer.common.data;

import com.github.zzt93.syncer.consumer.ack.Retryable;

/**
 * @author zzt
 */
public class SyncWrapper<T> implements Retryable {

  private final String syncDataId;
  private final String sourceId;
  private final T data;
  private int count = 0;

  public SyncWrapper(SyncData event, T data) {
    this.syncDataId = event.getDataId();
    this.sourceId = event.getSourceIdentifier();
    this.data = data;
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

  @Override
  public void inc() {
    count++;
  }

  @Override
  public int count() {
    return count;
  }
}
