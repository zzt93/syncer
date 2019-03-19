package com.github.zzt93.syncer.producer.dispatch;

import java.util.HashMap;

/**
 * @author zzt
 */
public interface NamedChange {

  default HashMap<String, Object> getFull() {
    throw new UnsupportedOperationException();
  }

  default HashMap<String, Object> getUpdated() {
    throw new UnsupportedOperationException();
  }

  default HashMap<String, Object> getBeforeFull() {
    throw new UnsupportedOperationException();
  }
}
