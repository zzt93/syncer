package com.github.zzt93.syncer.common.exception;

import com.github.zzt93.syncer.common.data.SyncData; /**
 * @author zzt
 */
public class InvalidSyncDataException extends RuntimeException {

  private final SyncData data;

  public InvalidSyncDataException(String s, SyncData data) {
    super(s);
    this.data = data;
  }

  public SyncData getData() {
    return data;
  }
}
