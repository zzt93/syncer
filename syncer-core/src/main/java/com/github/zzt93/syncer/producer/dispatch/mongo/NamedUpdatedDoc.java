package com.github.zzt93.syncer.producer.dispatch.mongo;

import com.github.zzt93.syncer.producer.dispatch.NamedChange;

import java.util.HashMap;

/**
 * @author zzt
 */
public class NamedUpdatedDoc implements NamedChange {

  private final HashMap<String, Object> updated;

  NamedUpdatedDoc(HashMap<String, Object> updated) {
    this.updated = updated;
  }

  @Override
  public HashMap<String, Object> getUpdated() {
    return updated;
  }

  @Override
  public HashMap<String, Object> getFull() {
    return updated;
  }

  @Override
  public HashMap<String, Object> getBeforeFull() {
    return null;
  }
}
