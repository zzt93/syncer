package com.github.zzt93.syncer.consumer.output.channel;

import com.github.zzt93.syncer.common.data.SyncWrapper;

import java.util.List;

/**
 * @author zzt
 */
public interface AckChannel<T> {

  void ackSuccess(List<SyncWrapper<T>> aim);

  void retryFailed(List<SyncWrapper<T>> aim, Exception e);

  boolean retriable(Exception e);

  boolean checkpoint();

}
