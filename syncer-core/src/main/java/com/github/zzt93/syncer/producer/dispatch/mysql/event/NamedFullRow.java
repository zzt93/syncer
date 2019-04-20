package com.github.zzt93.syncer.producer.dispatch.mysql.event;

import com.github.zzt93.syncer.common.thread.NotThreadSafe;
import com.github.zzt93.syncer.producer.dispatch.NamedChange;

import java.util.*;

/**
 * @author zzt
 */
public class NamedFullRow implements NamedChange {

  private final HashMap<String, Object> full;
  private HashMap<String, Object> beforeFull;
  private HashSet<String> updated;

  public NamedFullRow(HashMap<String, Object> full) {
    this.full = full;
  }

  public NamedFullRow setBeforeFull(HashMap<String, Object> beforeFull) {
    this.beforeFull = beforeFull;
    return this;
  }

  public Object get(String name) {
    return full.get(name);
  }

  public void remove(String name) {
    full.remove(name);
    if (beforeFull != null) {
      beforeFull.remove(name);
    }
  }

  @Override
  public HashMap<String, Object> getFull() {
    return full;
  }

  @Override
  public HashMap<String, Object> getBeforeFull() {
    return beforeFull;
  }

  @NotThreadSafe
  @Override
  public Set<String> getUpdated() {
    if (updated == null && beforeFull != null) {
      updated = new HashSet<>(full.size());
      for (Map.Entry<String, Object> e : full.entrySet()) {
        if (!Objects.deepEquals(beforeFull.get(e.getKey()), e.getValue())) {
          updated.add(e.getKey());
        }
      }
    }
    return updated;
  }

  @Override
  public String toString() {
    return "NamedFullRow{" +
        "full=" + full +
        ", beforeFull=" + beforeFull +
        ", updated=" + updated +
        '}';
  }
}
