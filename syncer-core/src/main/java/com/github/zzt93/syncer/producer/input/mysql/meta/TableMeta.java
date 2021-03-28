package com.github.zzt93.syncer.producer.input.mysql.meta;

import java.util.*;

/**
 * @author zzt
 */
public class TableMeta {

  private final List<Integer> interestedAndPkIndex = new ArrayList<>();
  private final HashMap<Integer, String> interestedAndPkIndexToName = new HashMap<>();
  private final Set<Integer> primaryKeys = new HashSet<>();
  private final Set<String> primaryKeysName = new HashSet<>();
  private boolean interestedPK = true;
  private volatile boolean coldStart;

  public TableMeta(boolean coldStart) {
    this.coldStart = coldStart;
  }

  void addInterestedCol(String columnName, int ordinalPosition) {
    interestedAndPkIndex.add(ordinalPosition);
    interestedAndPkIndexToName.put(ordinalPosition, columnName);
  }

  void addPrimaryKey(String columnName, int position) {
    primaryKeys.add(position);
    primaryKeysName.add(columnName);
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

  public void update(TableMeta all) {
    Map<String, Integer> name2index = new HashMap<>();
    for (Map.Entry<Integer, String> e : all.interestedAndPkIndexToName.entrySet()) {
      name2index.put(e.getValue(), e.getKey());
    }

    ArrayList<String> names = new ArrayList<>(interestedAndPkIndexToName.values());
    interestedAndPkIndexToName.clear();
    for (String name : names) {
      interestedAndPkIndexToName.put(name2index.get(name), name);
    }
    interestedAndPkIndex.clear();
    interestedAndPkIndex.addAll(interestedAndPkIndexToName.keySet());
    primaryKeys.clear();
    for (String name : primaryKeysName) {
      primaryKeys.add(name2index.get(name));
    }
  }

  public boolean isAll() {
    return false;
  }

  public boolean isColdStarting() {
    return coldStart;
  }
}
