package com.github.zzt93.syncer.producer.dispatch.mysql.event;

import com.github.zzt93.syncer.producer.dispatch.NamedChange;

import java.util.HashMap;

/**
 * @author zzt
 */
public class NamedFullRow implements NamedChange {

  private final HashMap<String, Object> full;
  private HashMap<String, Object> beforeFull;

  public NamedFullRow(HashMap<String, Object> full) {
    this.full = full;
  }

  NamedFullRow setBeforeFull(HashMap<String, Object> beforeFull) {
    this.beforeFull = beforeFull;
    return this;
  }

  public Object get(String name) {
    return full.get(name);
  }

  public void remove(String name) {
    full.remove(name);
    if (beforeFull != null) {
      full.remove(name);
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

  @Override
  public HashMap<String, Object> getUpdated() {
    return null;
  }
}
