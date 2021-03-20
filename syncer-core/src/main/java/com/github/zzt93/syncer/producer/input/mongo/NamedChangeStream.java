package com.github.zzt93.syncer.producer.input.mongo;

import com.github.zzt93.syncer.producer.dispatch.NamedChange;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author zzt
 */
public class NamedChangeStream implements NamedChange {

  private final HashMap<String, Object> full;
  private final HashMap<String, Object> updated;

  NamedChangeStream(HashMap<String, Object> full, HashMap<String, Object> updated) {
    this.full = full;
    this.updated = updated;
  }

  @Override
  public Set<String> getUpdated() {
    return updated != null ? updated.keySet() : null;
  }

  @Override
  public Map<String, Object> getFull() {
    return full;
  }

  @Override
  public HashMap<String, Object> getBeforeFull() {
    return null;
  }
}
