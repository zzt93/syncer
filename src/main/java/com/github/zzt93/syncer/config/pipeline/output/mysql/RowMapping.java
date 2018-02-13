package com.github.zzt93.syncer.config.pipeline.output.mysql;

import java.util.HashMap;

/**
 * @author zzt
 */
public class RowMapping {


  private String schema = "schema";
  private String table = "table";
  private String id = "id";
  private HashMap<String, Object> rows = new HashMap<>();

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

  public HashMap<String, Object> getRows() {
    return rows;
  }

  public void setRows(HashMap<String, Object> rows) {
    this.rows = rows;
  }
}
