package com.github.zzt93.syncer.producer.input.mongo;

import com.github.zzt93.syncer.producer.dispatch.NamedChange;

import java.util.HashMap;
import java.util.Set;

/**
 * @author zzt
 */
public class NamedUpdatedDoc implements NamedChange {

  private final HashMap<String, Object> updated;

  NamedUpdatedDoc(HashMap<String, Object> updated) {
    this.updated = updated;
  }

  @Override
  public Set<String> getUpdated() {
    return updated.keySet();
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
