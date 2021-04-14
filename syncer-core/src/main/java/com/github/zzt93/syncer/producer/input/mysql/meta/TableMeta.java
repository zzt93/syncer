package com.github.zzt93.syncer.producer.input.mysql.meta;

import com.github.zzt93.syncer.config.consumer.input.Entity;
import lombok.ToString;

import java.util.*;

/**
 * @author zzt
 */
@ToString
public class TableMeta {

  private final List<Integer> interestedAndPkIndex = new ArrayList<>();
  private final HashMap<Integer, String> interestedAndPkIndexToName = new HashMap<>();
  private int primaryKey;
  private String primaryKeysName;
  private boolean interestedPK = true;
  private final Entity coldStart;

  public TableMeta(Entity coldStart) {
    this.coldStart = coldStart;
  }

  void addInterestedCol(String columnName, int ordinalPosition) {
    interestedAndPkIndex.add(ordinalPosition);
    interestedAndPkIndexToName.put(ordinalPosition, columnName);
  }

  void addPrimaryKey(String columnName, int position) {
    primaryKey = position;
    primaryKeysName = columnName;
    addInterestedCol(columnName, position);
  }

  public List<Integer> getInterestedAndPkIndex() {
    return Collections.unmodifiableList(interestedAndPkIndex);
  }

  public Map<Integer, String> getInterestedAndPkIndexToName() {
    return Collections.unmodifiableMap(interestedAndPkIndexToName);
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
    primaryKey = name2index.get(primaryKeysName);
  }

  public boolean isAll() {
    return false;
  }

  public Entity coldStart() {
    return coldStart;
  }

  public boolean isCodeStart() {
    return coldStart.isCodeStart();
  }

  public String getPrimaryKeyName() {
    return primaryKeysName;
  }
}
