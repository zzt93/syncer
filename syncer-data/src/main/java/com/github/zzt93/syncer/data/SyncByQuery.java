package com.github.zzt93.syncer.data;


import java.util.HashMap;

/**
 * update(set field)/delete by query
 * @see ExtraQuery
 */
public class SyncByQuery {

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
