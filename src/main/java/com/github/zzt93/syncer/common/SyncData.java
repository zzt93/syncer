package com.github.zzt93.syncer.common;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

/**
 * @author zzt
 */
public class SyncData {

  private final EventType type;
  private final String schema;
  private final String table;
  private final String id;
  private final HashMap<String, Object> row = new HashMap<>();
  private final HashMap<String, Object> extra = new HashMap<>();

  private final Logger logger = LoggerFactory.getLogger(SyncData.class);
  private final StandardEvaluationContext context;

  public SyncData(TableMapEventData tableMap, HashMap<String, Object> data,
      EventType type) {
    this.type = type;
    Assert.isTrue(data.containsKey("id"), "[Assertion Failure]: no id in data");
    id = data.get("id").toString();
    schema = tableMap.getDatabase();
    table = tableMap.getTable();
    row.putAll(data);
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

  public void updateColumn(String column, Object value) {
    if (row.containsKey(column)) {
      row.put(column, value);
    } else {
      logger.warn("No such row name: {} in {}.{}", column, schema, table);
    }
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
