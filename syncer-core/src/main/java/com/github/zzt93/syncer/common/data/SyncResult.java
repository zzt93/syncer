package com.github.zzt93.syncer.common.data;

import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.data.SyncResultBase;
import lombok.ToString;

import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * @author zzt
 */
@ToString(callSuper = true)
public class SyncResult extends SyncResultBase {

  SyncResult() {
    fields = new LinkedHashMap<>();
  }

  SyncResult(SimpleEventType type, String database, String entity, String primaryKeyName, Object id,
             HashMap<String, Object> full) {
    fields = new LinkedHashMap<>(full);
    setPrimaryKeyName(primaryKeyName);
    setId(id);
    setEntity(entity);
    setRepo(database);
    setEventType(type);
  }

  Object getExtra(String key) {
    return extras != null ? extras.get(key) : null;
  }

  Object getBefore(String key) {
    return before != null ? before.get(key) : null;
  }

  /**
   * Not thread-safe, should not be invoked by multiple thread
   * @return one instance for this SyncResult
   */
  public LinkedHashMap<String, Object> getExtras() {
    if (extras == null) {
      extras = new LinkedHashMap<>();
    }
    return extras;
  }

}
