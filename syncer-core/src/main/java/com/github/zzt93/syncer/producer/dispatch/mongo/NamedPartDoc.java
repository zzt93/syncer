package com.github.zzt93.syncer.producer.dispatch.mongo;

import com.github.zzt93.syncer.producer.dispatch.NamedChange;

import java.util.HashMap;

/**
 * @author zzt
 */
public class NamedPartDoc implements NamedChange {

  private final HashMap<String, Object> part;

  NamedPartDoc(HashMap<String, Object> part) {
    this.part = part;
  }

  @Override
  public HashMap<String, Object> getPart() {
    return part;
  }

  @Override
  public HashMap<String, Object> getFull() {
    return part;
  }
}
