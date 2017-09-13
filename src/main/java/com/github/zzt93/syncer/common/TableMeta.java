package com.github.zzt93.syncer.common;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zzt
 */
public class TableMeta {

  private final List<Integer> index = new ArrayList<>();

  public List<Integer> getIndex() {
    return index;
  }

  public void addNameIndex(int ordinalPosition) {
    index.add(ordinalPosition);
  }
}
