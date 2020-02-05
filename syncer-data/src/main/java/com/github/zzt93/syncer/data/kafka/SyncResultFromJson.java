package com.github.zzt93.syncer.data.kafka;

import com.github.zzt93.syncer.data.SyncResultBase;
import lombok.ToString;

/**
 * @author zzt
 */
@ToString(callSuper = true)
public class SyncResultFromJson extends SyncResultBase {

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

  public Long getFieldAsLong(String key) {
    Object res = getFields().get(key);
    if (res != null) {
      return ((Double) res).longValue();
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
