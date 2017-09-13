package com.github.zzt93.syncer.config.common;

import java.util.HashMap;

/**
 * @author zzt
 */
public class TableMeta {

  private final HashMap<String, Integer> nameToIndex = new HashMap<>();

  public TableMeta addNameIndex(String name, int i) {
    nameToIndex.put(name, i);
    return this;
  }
}
