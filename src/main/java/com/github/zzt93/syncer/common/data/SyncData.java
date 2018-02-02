package com.github.zzt93.syncer.common.data;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.zzt93.syncer.common.IdGenerator;
import com.github.zzt93.syncer.common.IdGenerator.Offset;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

/**
 * @author zzt
 */
public class SyncData {

  private final transient Logger logger = LoggerFactory.getLogger(SyncData.class);

  private static class Meta {
    private final String eventId;
    private final String dataId;
    private final EventType type;
    private final String action;
    private final StandardEvaluationContext context;
    private final int ordinal;
    private boolean hasExtra = false;
    private String connectionIdentifier;

    Meta(String eventId, int ordinal, EventType type, String connectionIdentifier,
        StandardEvaluationContext context) {
      this.eventId = eventId;
      this.connectionIdentifier = connectionIdentifier;
      dataId = IdGenerator.fromEventId(eventId, ordinal);
      this.ordinal = ordinal;
      this.type = type;
      this.action = type.toString();
      this.context = context;
    }
  }
  private SyncByQueryES syncByQuery = new SyncByQueryES();

  private final Meta inner;
  /*
   * The following is data field
   */
  private final HashMap<String, Object> records = new HashMap<>();
  private final HashMap<String, Object> extra = new HashMap<>();
  private String schema;
  private String table;
  /**
   * table primary key
   */
  private Object id;
  private String primaryKeyName;


  public SyncData(String eventId, int ordinal, String database, String table, String primaryKeyName,
      Map<String, Object> row, EventType type) {
    inner = new Meta(eventId, ordinal, type, null, new StandardEvaluationContext(this));

    Object key = row.get(primaryKeyName);
    this.primaryKeyName = primaryKeyName;
    if (key != null) {
      id = key;
    } else {
      logger.warn("{} without primary key", type);
    }
    schema = database;
    this.table = table;
    records.putAll(row);
  }

  public SyncData(SyncData syncData, Offset offset) {
    inner = new Meta(syncData.getEventId(), syncData.inner.ordinal + offset.getOffset(),
        syncData.getType(),
        syncData.getSourceIdentifier(), new StandardEvaluationContext(this));
  }

  public Object getId() {
    return id;
  }

  public void setId(Object id) {
    this.id = id;
  }

  public String getTable() {
    return table;
  }

  public String getAction() {
    return inner.action;
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
    return inner.type;
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
      logger.warn("No such record name (maybe filtered out): `{}` in `{}`.`{}`", oldKey, schema, table);
    }
  }

  public void removeRecord(String key) {
    records.remove(key);
  }

  public void removePrimaryKey() {
    if (primaryKeyName!=null) {
      records.remove(primaryKeyName);
    }
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
      logger.warn("No such record name (check your config): {} in {}.{}", key, schema, table);
    }
  }

  public StandardEvaluationContext getContext() {
    return inner.context;
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
    return inner.eventId;
  }

  public String getDataId() {
    return inner.dataId;
  }

  public SyncData setSourceIdentifier(String identifier) {
    inner.connectionIdentifier = identifier;
    return this;
  }

  public String getSourceIdentifier() {
    return inner.connectionIdentifier;
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
    if (inner.hasExtra) {
      logger.warn("Multiple extraQuery, not support");
    }
    inner.hasExtra = true;
    return new ExtraQuery(this).setIndexName(indexName).setTypeName(typeName);
  }

  public boolean hasExtra() {
    return inner.hasExtra;
  }

  @Override
  public String toString() {
    return "SyncData{" +
        "eventId='" + inner.eventId + '\'' +
        ", type=" + inner.type +
        ", action='" + inner.action + '\'' +
        ", records=" + records +
        ", extra=" + extra +
        ", context=" + inner.context +
        ", schema='" + schema + '\'' +
        ", table='" + table + '\'' +
        ", id='" + id + '\'' +
        ", syncByQuery=" + syncByQuery +
        '}';
  }
}
