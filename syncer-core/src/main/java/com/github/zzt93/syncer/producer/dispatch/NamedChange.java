package com.github.zzt93.syncer.producer.dispatch;

import com.github.zzt93.syncer.config.consumer.input.MasterSourceType;
import com.github.zzt93.syncer.data.SimpleEventType;

import java.util.HashMap;
import java.util.Set;

/**
 * @author zzt
 */
public interface NamedChange {

  default HashMap<String, Object> getFull() {
    throw new UnsupportedOperationException();
  }

  /**
   * Only {@link SimpleEventType#UPDATE} will have this,
   * otherwise return null
   * @return
   */
  default Set<String> getUpdated() {
    throw new UnsupportedOperationException();
  }

  /**
   * Only {@link MasterSourceType#MySQL} && {@link SimpleEventType#UPDATE} will have this,
   * otherwise return null
   */
  default HashMap<String, Object> getBeforeFull() {
    throw new UnsupportedOperationException();
  }
}
