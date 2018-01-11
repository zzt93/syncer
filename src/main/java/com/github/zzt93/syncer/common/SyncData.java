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

  private final String eventId;

  private final Logger logger = LoggerFactory.getLogger(SyncData.class);

  private EventType type;
  private String action;
  /*
   * The following is data field
   */
  private final HashMap<String, Object> records = new HashMap<>();
  private final HashMap<String, Object> extra = new HashMap<>();
  private final StandardEvaluationContext context;
  private String schema;
  private String table;
  /**
   * table primary key
   */
  private String id;

  private SyncByQueryES syncByQuery = new SyncByQueryES();
  private boolean hasExtra = false;


  public SyncData(String eventId, TableMapEventData tableMap, String primaryKey,
      HashMap<String, Object> row, EventType type) {
    this.eventId = eventId;
    this.type = type;
    action = type.toString();
    Object key = row.get(primaryKey);
    if (key != null) {
      id = key.toString();
    }
    schema = tableMap.getDatabase();
    table = tableMap.getTable();
    records.putAll(row);
    context = new StandardEvaluationContext(this);
  }

  public SyncData(String eventId) {
    this.eventId = eventId;
    context = new StandardEvaluationContext(this);
  }

  public SyncData setType(EventType type) {
    this.type = type;
    return this;
  }

  public SyncData setAction(String action) {
    this.action = action;
    return this;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getTable() {
    return table;
  }

  public String getAction() {
    return action;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public EventType getType() {
    return type;
  }

  public void addExtra(String key, Object value) {
    extra.put(key, value);
  }

  public SyncData addRecord(String key, Object value) {
    if (value == null) {
      logger.warn("Adding column({}) with null, discarded", key);
    }
    records.put(key, value);
    return this;
  }

  public void renameRecord(String oldKey, String newKey) {
    if (records.containsKey(oldKey)) {
      records.put(newKey, records.get(oldKey));
      records.remove(oldKey);
    } else {
      logger.warn("No such record name (maybe filtered out): {} in {}.{}", oldKey, schema, table);
    }
  }

  public void removeRecord(String key) {
    records.remove(key);
  }

  public void removeRecords(String... keys) {
    for (String colName : keys) {
      records.remove(colName);
    }
  }

  public boolean containRecord(String key) {
    return records.containsKey(key);
  }

  public void updateRecord(String key, Object value) {
    if (records.containsKey(key)) {
      records.put(key, value);
    } else {
      logger.warn("No such record name (maybe filtered out): {} in {}.{}", key, schema, table);
    }
  }

  public StandardEvaluationContext getContext() {
    return context;
  }

  public HashMap<String, Object> getRecords() {
    return records;
  }

  public HashMap<String, Object> getExtra() {
    return extra;
  }

  public Object getRecordValue(String key) {
    Assert.isTrue(records.containsKey(key), records.toString() + "[No such record]: " + key);
    return records.get(key);
  }

  public String getEventId() {
    return eventId;
  }

  public HashMap<String, Object> getSyncBy() {
    return syncByQuery.getSyncBy();
  }

  public boolean isSyncWithoutId() {
    return syncByQuery.isSyncWithoutId();
  }

  /*--------------------*/

  /**
   * update/delete by query
   */
  public SyncByQueryES syncByQuery() {
    return syncByQuery;
  }

  public ExtraQuery extraQuery(String indexName, String typeName) {
    if (hasExtra) {
      logger.warn("Multiple extraQuery, not support");
    }
    hasExtra = true;
    return new ExtraQuery(this).setIndexName(indexName).setTypeName(typeName);
  }

  public boolean hasExtra() {
    return hasExtra;
  }

  @Override
  public String toString() {
    return "SyncData{" +
        "eventId='" + eventId + '\'' +
        ", type=" + type +
        ", action='" + action + '\'' +
        ", records=" + records +
        ", extra=" + extra +
        ", context=" + context +
        ", schema='" + schema + '\'' +
        ", table='" + table + '\'' +
        ", id='" + id + '\'' +
        ", syncByQuery=" + syncByQuery +
        '}';
  }
}
