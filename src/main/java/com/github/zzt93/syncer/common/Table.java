package com.github.zzt93.syncer.common;

import java.util.List;

/**
 * @author zzt
 */
public class Table {

  private String name;
  private List<String> rowName;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<String> getRowName() {
    return rowName;
  }

  public void setRowName(List<String> rowName) {
    this.rowName = rowName;
  }
}
