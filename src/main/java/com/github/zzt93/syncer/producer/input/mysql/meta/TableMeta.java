package com.github.zzt93.syncer.producer.input.mysql.meta;

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

  private final List<Integer> interestedAndPkIndex = new ArrayList<>();
  private final HashMap<Integer, String> interestedAndPkIndexToName = new HashMap<>();
  private final Set<Integer> primaryKeys = new HashSet<>();
  private boolean interestedPK = true;

  void addInterestedCol(String columnName, int ordinalPosition) {
    interestedAndPkIndex.add(ordinalPosition);
    interestedAndPkIndexToName.put(ordinalPosition, columnName);
  }

  void addPrimaryKey(String columnName, int position) {
    primaryKeys.add(position);
    addInterestedCol(columnName, position);
  }

  public List<Integer> getInterestedAndPkIndex() {
    return Collections.unmodifiableList(interestedAndPkIndex);
  }

  public Map<Integer, String> getInterestedAndPkIndexToName() {
    return Collections.unmodifiableMap(interestedAndPkIndexToName);
  }

  public Set<Integer> getPrimaryKeys() {
    return Collections.unmodifiableSet(primaryKeys);
  }

  @Override
  public String toString() {
    return "TableMeta{" +
        "interestedAndPkIndex=" + interestedAndPkIndex +
        ", interestedAndPkIndexToName=" + interestedAndPkIndexToName +
        ", primaryKeys=" + primaryKeys +
        ", interestedPK=" + interestedPK +
        '}';
  }

  void noPrimaryKey() {
    interestedPK = false;
  }

  public boolean isInterestedPK() {
    return interestedPK;
  }
}
