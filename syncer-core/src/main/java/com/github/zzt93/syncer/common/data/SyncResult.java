package com.github.zzt93.syncer.common.data;

import com.github.zzt93.syncer.data.SyncResultBase;
import lombok.ToString;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author zzt
 */
@ToString(callSuper = true)
public class SyncResult extends SyncResultBase {

  SyncResult() {
    fields = new LinkedHashMap<>();
  }

  SyncResult(Map<String, Object> row) {
    fields = new LinkedHashMap<>(row);
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
