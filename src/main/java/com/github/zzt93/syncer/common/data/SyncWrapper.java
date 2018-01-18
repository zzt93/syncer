package com.github.zzt93.syncer.common.data;

/**
 * @author zzt
 */
public class SyncWrapper<T> {

  private final String syncDataId;
  private final String sourceId;
  private final T data;

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
}
