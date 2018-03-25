package com.github.zzt93.syncer.common.data;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.zzt93.syncer.common.IdGenerator;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.expression.MapAccessor;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

/**
 * @author zzt
 */
public class SyncData {

  private final transient Logger logger = LoggerFactory.getLogger(SyncData.class);

  private static class Meta {
    private final static MapAccessor accessor = new MapAccessor();
    private final String eventId;
    private final String dataId;
    private final int ordinal;
    private EventType type;
    private String action;
    private final StandardEvaluationContext context;
    private boolean hasExtra = false;
    private String connectionIdentifier;

    Meta(String eventId, int ordinal, int offset, EventType type, String connectionIdentifier,
        StandardEvaluationContext context) {
      this.eventId = eventId;
      this.connectionIdentifier = connectionIdentifier;
      if (offset < 0) {
        dataId = IdGenerator.fromEventId(eventId, ordinal);
      } else {
        dataId = IdGenerator.fromEventId(eventId, ordinal, offset);
      }
      this.ordinal = ordinal;
      setType(type);
      this.context = context;
      context.setTypeLocator(new CommonTypeLocator());
      context.addPropertyAccessor(accessor);
    }

    @Override
    public String toString() {
      return "Meta{" +
          "eventId='" + eventId + '\'' +
          ", dataId='" + dataId + '\'' +
          ", ordinal=" + ordinal +
          ", type=" + type +
          ", action='" + action + '\'' +
          ", context=" + context +
          ", hasExtra=" + hasExtra +
          ", connectionIdentifier='" + connectionIdentifier + '\'' +
          '}';
    }

    private void setType(EventType type) {
      this.type = type;
      action = type.toString();
    }
  }
  private SyncByQuery syncByQuery;

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
    inner = new Meta(eventId, ordinal, -1, type, null, new StandardEvaluationContext(this));

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

  public SyncData(SyncData syncData, int offset) {
    inner = new Meta(syncData.getEventId(), syncData.inner.ordinal, offset,
        syncData.getType(),
        syncData.getSourceIdentifier(), new StandardEvaluationContext(this));
    if (syncData.syncByQuery != null) {
      syncByQuery = new SyncByQueryES(this);
    }
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

  public SyncData renameRecord(String oldKey, String newKey) {
    if (records.containsKey(oldKey)) {
      records.put(newKey, records.get(oldKey));
      records.remove(oldKey);
    } else {
      logger.warn("No such record name (maybe filtered out): `{}` in `{}`.`{}`", oldKey, schema, table);
    }
    return this;
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
      if (value != null) {
        records.put(key, value);
      } else {
        logger.warn("update record[{}] with null", key);
      }
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
    return syncByQuery != null && syncByQuery.isSyncWithoutId();
  }

  /**
   * update/delete by query
   */
  public SyncByQuery syncByQuery() {
    if (syncByQuery == null) {
      syncByQuery = new SyncByQueryES(this);
    }
    return syncByQuery;
  }

  public InsertByQuery insertByQuery(String indexName, String typeName) {
    if (inner.hasExtra) {
      logger.info("Multiple insert by query, not supported for mysql output");
    }
    inner.hasExtra = true;
    return new InsertByQuery(this).setIndexName(indexName).setTypeName(typeName);
  }

  public boolean hasExtra() {
    return inner.hasExtra;
  }

  void setEventType(EventType type) {
    inner.setType(type);
  }

  @Override
  public String toString() {
    return "SyncData{" +
        "syncByQuery=" + syncByQuery +
        ", inner=" + inner +
        ", records=" + records +
        ", extra=" + extra +
        ", schema='" + schema + '\'' +
        ", table='" + table + '\'' +
        ", id=" + id +
        ", primaryKeyName='" + primaryKeyName + '\'' +
        '}';
  }
}
