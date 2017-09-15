package com.github.zzt93.syncer.config.pipeline.input;

import java.util.List;

/**
 * @author zzt
 */
public class Table {

  private String name;
  private List<String> rowName;

  public Table() {
  }

  public Table(String tableName) {
    this.name = tableName;
  }

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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Table table = (Table) o;

    return name.equals(table.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }
}
