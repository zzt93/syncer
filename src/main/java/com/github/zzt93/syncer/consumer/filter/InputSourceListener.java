package com.github.zzt93.syncer.consumer.filter;

import com.github.zzt93.syncer.common.SyncData;

/**
 * @author zzt
 */
public interface InputSourceListener {

  default void onSyncData(SyncData syncData) {
    throw new UnsupportedOperationException("Not implemented");
  }

  default void onSyncData(SyncData[] syncData) {
    throw new UnsupportedOperationException("Not implemented");
  }

}
