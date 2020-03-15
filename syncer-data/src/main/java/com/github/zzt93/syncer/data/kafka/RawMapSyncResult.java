package com.github.zzt93.syncer.data.kafka;

import com.github.zzt93.syncer.data.SyncResultBase;
import lombok.ToString;

/**
 * @author zzt
 */
@ToString(callSuper = true)
public class RawMapSyncResult extends SyncResultBase {

  public Object getExtra(String key) {
    return extras != null ? extras.get(key) : null;
  }

  public Object getBefore(String key) {
    return before != null ? before.get(key) : null;
  }

  public Integer getFieldAsInt(String key) {
    Object res = getFields().get(key);
    if (res != null) {
      return ((Double) res).intValue();
    }
    return null;
  }

  public Long getIdAsLong() {
    return getLong(getId());
  }

  public Long getFieldAsLong(String key) {
    return getLong(getFields().get(key));
  }

  private static Long getLong(Object res) {
    if (res != null) {
      try {
        return Long.parseLong((String) res);
      } catch (ClassCastException e) {
        if (res instanceof Double) { // for backward compatible
          return ((Double) res).longValue();
        }
      }
    }
    return null;
  }

  public String getFieldAsStr(String key) {
    Object res = getFields().get(key);
    if (res != null) {
      return (String) res;
    }
    return null;
  }

}
