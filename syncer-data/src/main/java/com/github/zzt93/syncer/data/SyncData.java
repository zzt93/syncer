package com.github.zzt93.syncer.data;

import com.github.shyiko.mysql.binlog.event.EventType;

import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * @author zzt
 */
public class SyncData {

  private static class Meta {
    private String eventId;
    private String dataId;
    private int ordinal;
    private EventType type;
    private boolean hasExtra = false;
    private String connectionIdentifier;

    @Override
    public String toString() {
      return "Meta{" +
          "eventId='" + eventId + '\'' +
          ", dataId='" + dataId + '\'' +
          ", ordinal=" + ordinal +
          ", type=" + type +
          ", hasExtra=" + hasExtra +
          ", connectionIdentifier='" + connectionIdentifier + '\'' +
          '}';
    }

    private void setType(EventType type) {
      this.type = type;
    }
  }
  private SyncByQuery syncByQuery;

  private Meta inner;
  /*
   * The following is data field
   */
  /**
   * fields have to use `LinkedHashMap` to be in order to support multiple dependent extraQuery
   */
  private final HashMap<String, Object> fields = new LinkedHashMap<>();
  private final HashMap<String, Object> extra = new HashMap<>();
  private String schema;
  private String table;
  /**
   * table primary key
   */
  private Object id;
  private String primaryKeyName;

  public Object getId() {
    return id;
  }

  public void setId(Object id) {
    this.id = id;
  }

  public String getTable() {
    return table;
  }

  public boolean isWrite() {
    return EventType.isWrite(inner.type);
  }

  public boolean isUpdate() {
    return EventType.isUpdate(inner.type);
  }

  public boolean isDelete() {
    return EventType.isDelete(inner.type);
  }

  public boolean toWrite() {
    return updateType(EventType.WRITE_ROWS);
  }

  public boolean toUpdate() {
    return updateType(EventType.UPDATE_ROWS);
  }

  public boolean toDelete() {
    return updateType(EventType.DELETE_ROWS);
  }

  private boolean updateType(EventType type) {
    boolean res = inner.type == type;
    inner.setType(type);
    return res;
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

  public SyncData addField(String key, Object value) {
    fields.put(key, value);
    return this;
  }

  public SyncData renameField(String oldKey, String newKey) {
    if (fields.containsKey(oldKey)) {
      fields.put(newKey, fields.get(oldKey));
      fields.remove(oldKey);
    }
    return this;
  }

  public SyncData removeField(String key) {
    fields.remove(key);
    return this;
  }

  public boolean removePrimaryKey() {
    return primaryKeyName != null && fields.remove(primaryKeyName) != null;
  }

  public SyncData removeFields(String... keys) {
    for (String colName : keys) {
      fields.remove(colName);
    }
    return this;
  }

  public boolean containField(String key) {
    return fields.containsKey(key);
  }

  public SyncData updateField(String key, Object value) {
    if (fields.containsKey(key)) {
      if (value != null) {
        fields.put(key, value);
      }
    }
    return this;
  }

  public HashMap<String, Object> getFields() {
    return fields;
  }

  public HashMap<String, Object> getExtra() {
    return extra;
  }

  public Object getField(String key) {
    return fields.get(key);
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
    if (syncByQuery == null) {
      return null;
    }
    return syncByQuery.getSyncBy();
  }

  /**
   * update/delete by query
   */
  public SyncByQuery syncByQuery() {
    if (syncByQuery == null) {
      syncByQuery = new ESScriptUpdate(this);
    }
    return syncByQuery;
  }

  public ExtraQuery extraQuery(String indexName, String typeName) {
    inner.hasExtra = true;
    return new ExtraQuery(this).setIndexName(indexName).setTypeName(typeName);
  }

  public boolean hasExtra() {
    return inner.hasExtra;
  }

  @Override
  public String toString() {
    return "SyncData{" +
        "syncByQuery=" + syncByQuery +
        ", inner=" + inner +
        ", fields=" + fields +
        ", extra=" + extra +
        ", schema='" + schema + '\'' +
        ", table='" + table + '\'' +
        ", id=" + id +
        ", primaryKeyName='" + primaryKeyName + '\'' +
        '}';
  }
}
