package com.github.zzt93.syncer.consumer.output.channel;

import com.github.zzt93.syncer.consumer.ack.Ack;

import java.util.List;

/**
 * @author zzt
 */
public interface AckChannel<T> {

  default void ackSuccess(List<SyncWrapper<T>> aim) {
    for (SyncWrapper<T> wrapper : aim) {
      getAck().remove(wrapper.getSourceId(), wrapper.getSyncDataId());
    }
  }

  Ack getAck();

  void retryFailed(List<SyncWrapper<T>> aim, Throwable e);

  default ErrorLevel level(Throwable e, SyncWrapper wrapper, int maxTry) {
    if (wrapper.retryCount() >= maxTry) {
      return ErrorLevel.MAX_TRY_EXCEED;
    }
    return ErrorLevel.RETRIABLE_ERROR;
  }

  default boolean checkpoint() {
    return getAck().flush();
  }

  enum ErrorLevel {
    MAX_TRY_EXCEED,
    WARN,
    SYNCER_BUG,
    RETRIABLE_ERROR {
      @Override
      public boolean retriable() {
        return true;
      }
    },
    ;

    public boolean retriable() {
      return false;
    }
  }
}
