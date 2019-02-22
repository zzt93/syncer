package com.github.zzt93.syncer.config.consumer.output.mysql;

import java.util.LinkedHashMap;

/**
 * @author zzt
 */
public class RowMapping {


  private String schema = "repo";
  private String table = "entity";
  private String id = "id";
  private LinkedHashMap<String, Object> rows = new LinkedHashMap<>();

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public LinkedHashMap<String, Object> getRows() {
    return rows;
  }

  public void setRows(LinkedHashMap<String, Object> rows) {
    this.rows = rows;
  }
}
