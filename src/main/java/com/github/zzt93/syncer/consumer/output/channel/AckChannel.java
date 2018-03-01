package com.github.zzt93.syncer.consumer.output.channel;

import com.github.zzt93.syncer.common.data.SyncWrapper;

/**
 * @author zzt
 */
public interface AckChannel {

  void ackSuccess(SyncWrapper[] wrappers);

  void retryFailed(SyncWrapper[] wrappers, Exception e);

  boolean retriable(Exception e);

}
