package com.github.zzt93.syncer.common.data;

import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ----------- update/delete by query -----------
 */
public class SyncByQueryES {

  private static final Logger logger = LoggerFactory.getLogger(SyncByQueryES.class);

  private final HashMap<String, Object> syncBy = new HashMap<>();

  public SyncByQueryES filter(String syncWithCol, Object value) {
    syncBy.put(syncWithCol, value);
    return this;
  }

  boolean isSyncWithoutId() {
    return !syncBy.isEmpty();
  }

  HashMap<String, Object> getSyncBy() {
    return syncBy;
  }

}
