package com.github.zzt93.syncer.consumer.output.channel;

import com.github.zzt93.syncer.common.data.SyncWrapper;
import java.util.List;

/**
 * @author zzt
 */
public interface AckChannel<T> {

  void ackSuccess(List<SyncWrapper<T>> aim);

  void retryFailed(List<SyncWrapper<T>> aim, Exception e);

  default ErrorLevel level(Exception e) {
    return ErrorLevel.RETRIABLE_ERROR;
  }

  boolean checkpoint();

  enum ErrorLevel {
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
