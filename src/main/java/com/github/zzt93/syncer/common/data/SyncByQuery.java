package com.github.zzt93.syncer.common.data;

import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * update(set field)/delete by query
 * @see InsertByQuery
 */
public class SyncByQuery {

  private static final Logger logger = LoggerFactory.getLogger(SyncByQuery.class);

  private final HashMap<String, Object> syncBy = new HashMap<>();

  public SyncByQuery filter(String syncWithCol, Object value) {
    syncBy.put(syncWithCol, value);
    return this;
  }

  boolean isSyncWithoutId() {
    return !syncBy.isEmpty();
  }

  HashMap<String, Object> getSyncBy() {
    return syncBy;
  }

  @Override
  public String toString() {
    return "SyncByQuery{" +
        "syncBy=" + syncBy +
        '}';
  }
}
