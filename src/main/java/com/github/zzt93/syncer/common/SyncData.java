package com.github.zzt93.syncer.common;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

  public SyncData(TableMapEventData tableMap, HashMap<String, Object> data,
      EventType type) {
    this.type = type;
    Assert.isTrue(data.containsKey("id"), "[Assertion Failure]: no id in data");
    id = data.get("id").toString();
    schema = tableMap.getDatabase();
    table = tableMap.getTable();
    row.putAll(data);
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

  public void addExtra(String colName, Object value) {
    extra.put(colName, value);
  }

  public void addRow(String colName, Object value) {
    row.put(colName, value);
  }

  public void renameRow(String oldKey, String newKey) {
    row.put(newKey, row.get(oldKey));
    row.remove(oldKey);
  }

  public void removeRow(String rowName) {
    row.remove(rowName);
  }

  public HashMap<String, Object> getRow() {
    return row;
  }

  public HashMap<String, Object> getExtra() {
    return extra;
  }
}
