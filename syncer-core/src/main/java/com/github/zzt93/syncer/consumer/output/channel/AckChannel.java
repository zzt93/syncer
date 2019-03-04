package com.github.zzt93.syncer.consumer.output.channel;

import java.util.List;

/**
 * @author zzt
 */
public interface AckChannel<T> {

  void ackSuccess(List<SyncWrapper<T>> aim);

  void retryFailed(List<SyncWrapper<T>> aim, Throwable e);

  default ErrorLevel level(Throwable e, SyncWrapper wrapper, int maxTry) {
    if (wrapper.retryCount() >= maxTry) {
      return ErrorLevel.MAX_TRY_EXCEED;
    }
    return ErrorLevel.RETRIABLE_ERROR;
  }

  boolean checkpoint();

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
