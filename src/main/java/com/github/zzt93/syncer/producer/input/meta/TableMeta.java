package com.github.zzt93.syncer.producer.input.meta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author zzt
 */
public class TableMeta {

  private final List<Integer> index = new ArrayList<>();
  private final HashMap<Integer, String> indexToName = new HashMap<>();
  private final Set<Integer> primaryKeys = new HashSet<>();

  void addNameIndex(String columnName, int ordinalPosition) {
    index.add(ordinalPosition);
    indexToName.put(ordinalPosition, columnName);
  }

  void addPrimaryKey(int position) {
    primaryKeys.add(position);
  }

  public List<Integer> getInterestedColIndex() {
    return Collections.unmodifiableList(index);
  }

  public Map<Integer, String> getIndexToName() {
    return Collections.unmodifiableMap(indexToName);
  }

  public Set<Integer> getPrimaryKeys() {
    return Collections.unmodifiableSet(primaryKeys);
  }

  @Override
  public String toString() {
    return "TableMeta{" +
        "indexToName=" + indexToName +
        '}';
  }
}
