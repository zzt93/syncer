package com.github.zzt93.syncer.config.pipeline.output;

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

  public RowMapping setSchema(String schema) {
    this.schema = schema;
    return this;
  }

  public String getTable() {
    return table;
  }

  public RowMapping setTable(String table) {
    this.table = table;
    return this;
  }

  public String getId() {
    return id;
  }

  public RowMapping setId(String id) {
    this.id = id;
    return this;
  }

  public HashMap<String, Object> getRows() {
    return rows;
  }

  public RowMapping setRows(HashMap<String, Object> rows) {
    this.rows = rows;
    return this;
  }
}
