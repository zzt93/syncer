package com.github.zzt93.syncer.common;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * @author zzt
 */
public class SyncData {

  private final EventType type;
  private final HashMap<String, Object> row = new HashMap<>();
  private final HashMap<String, Object> extra = new HashMap<>();
  private final Logger logger = LoggerFactory.getLogger(SyncData.class);
  private final StandardEvaluationContext context;
  private String schema;
  private String table;
  /**
   * table primary key
   */
  private String id;
  private boolean syncWithoutId;

  public SyncData(TableMapEventData tableMap, String primaryKey,
      HashMap<String, Object> row, EventType type) {
    this.type = type;
    Object key = row.get(primaryKey);
    if (key != null) {
      id = key.toString();
    }
    schema = tableMap.getDatabase();
    table = tableMap.getTable();
    this.row.putAll(row);
    context = new StandardEvaluationContext(this);
  }

  public SyncData(EventType type) {
    this.type = type;
    context = new StandardEvaluationContext(this);
  }

  public String getId() {
    return id;
  }

  public String getTable() {
    return table;
  }

  public String getSchema() {
    return schema;
  }

  public EventType getType() {
    return type;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setSyncWithoutId(boolean syncWithoutId) {
    this.syncWithoutId = syncWithoutId;
  }

  public void addExtra(String key, Object value) {
    extra.put(key, value);
  }

  public void addRow(String colName, Object value) {
    row.put(colName, value);
  }

  public void renameColumn(String oldKey, String newKey) {
    if (row.containsKey(oldKey)) {
      row.put(newKey, row.get(oldKey));
      row.remove(oldKey);
    } else {
      logger.warn("No such row name: {} in {}.{}", oldKey, schema, table);
    }
  }

  public void removeColumn(String colName) {
    row.remove(colName);
  }

  public void removeColumns(String... colNames) {
    for (String colName : colNames) {
      row.remove(colName);
    }
  }

  public boolean containColumn(String col) {
    return row.containsKey(col);
  }

  public void updateColumn(String column, Object value) {
    if (row.containsKey(column)) {
      row.put(column, value);
    } else {
      logger.warn("No such row name: {} in {}.{}", column, schema, table);
    }
  }

  public void syncWithoutId() {
    syncWithoutId = true;
    id = null;
  }

  public boolean isSyncWithoutId() {
    return syncWithoutId;
  }

  public StandardEvaluationContext getContext() {
    return context;
  }

  public HashMap<String, Object> getRow() {
    return row;
  }

  public HashMap<String, Object> getExtra() {
    return extra;
  }
}
