package com.github.zzt93.syncer.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zzt
 */
public class TableMeta {

  private final List<Integer> index = new ArrayList<>();
  private final HashMap<Integer, String> indexToName = new HashMap<>();

  void addNameIndex(String columnName, int ordinalPosition) {
    index.add(ordinalPosition);
    indexToName.put(ordinalPosition, columnName);
  }

  public List<Integer> getIndex() {
    return Collections.unmodifiableList(index);
  }

  public Map<Integer, String> getIndexToName() {
    return Collections.unmodifiableMap(indexToName);
  }
}
