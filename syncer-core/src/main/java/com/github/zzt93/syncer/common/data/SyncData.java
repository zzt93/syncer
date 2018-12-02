package com.github.zzt93.syncer.common.data;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.zzt93.syncer.common.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author zzt
 */
public class SyncData implements Serializable {

  private final transient Logger logger = LoggerFactory.getLogger(SyncData.class);

  private static class Meta {
    private final String eventId;
    private final String dataId;
    private final int ordinal;
    private EventType type;
    private transient StandardEvaluationContext context;
    private boolean hasExtra = false;
    private String connectionIdentifier;

    Meta(String eventId, int ordinal, int offset, EventType type, String connectionIdentifier) {
      this.eventId = eventId;
      this.connectionIdentifier = connectionIdentifier;
      if (offset < 0) {
        dataId = IdGenerator.fromEventId(eventId, ordinal);
      } else {
        dataId = IdGenerator.fromEventId(eventId, ordinal, offset);
      }
      this.ordinal = ordinal;
      setType(type);
    }

    @Override
    public String toString() {
      return "Meta{" +
          "eventId='" + eventId + '\'' +
          ", dataId='" + dataId + '\'' +
          ", ordinal=" + ordinal +
          ", type=" + type +
          ", context=" + context +
          ", hasExtra=" + hasExtra +
          ", connectionIdentifier='" + connectionIdentifier + '\'' +
          '}';
    }

    private void setType(EventType type) {
      this.type = type;
    }
  }
  private SyncByQuery syncByQuery;

  private final Meta inner;
  /*
   * The following is data field
   */
  /**
   * {@link #fields} have to use `LinkedHashMap` to has order so as to support multiple dependent extraQuery
   */
  private final HashMap<String, Object> fields = new LinkedHashMap<>();
  private final HashMap<String, Object> extra = new HashMap<>();
  private String repo;
  private String entity;
  /**
   * table primary key
   */
  private Object id;
  private String primaryKeyName;


  public SyncData(String eventId, int ordinal, String database, String entity, String primaryKeyName,
                  Object id, Map<String, Object> row, EventType type) {
    inner = new Meta(eventId, ordinal, -1, type, null);

    this.primaryKeyName = primaryKeyName;
    if (id != null) {
      this.id = id;
    } else {
      logger.error("{} without primary key", type);
    }
    repo = database;
    this.entity = entity;
    fields.putAll(row);
  }

  public SyncData(SyncData syncData, int offset) {
    inner = new Meta(syncData.getEventId(), syncData.inner.ordinal, offset,
        syncData.getType(),
        syncData.getSourceIdentifier());
    inner.context = EvaluationFactory.context();
    inner.context.setRootObject(this);
  }

  public Object getId() {
    return id;
  }

  public void setId(Object id) {
    this.id = id;
  }

  public String getEntity() {
    return entity;
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

  public void setEntity(String entity) {
    this.entity = entity;
  }

  public String getRepo() {
    return repo;
  }

  public void setRepo(String repo) {
    this.repo = repo;
  }

  public EventType getType() {
    return inner.type;
  }

  public void addExtra(String key, Object value) {
    extra.put(key, value);
  }

  public SyncData addField(String key, Object value) {
    if (value == null) {
      logger.warn("Adding column({}) with null, discarded", key);
    }
    fields.put(key, value);
    return this;
  }

  public SyncData renameField(String oldKey, String newKey) {
    if (fields.containsKey(oldKey)) {
      fields.put(newKey, fields.get(oldKey));
      fields.remove(oldKey);
    } else {
      logger.warn("No such field name (maybe filtered out): `{}` in `{}`.`{}`", oldKey, repo, entity);
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
      } else {
        logger.warn("update field[{}] with null", key);
      }
    } else {
      logger.warn("No such field name (check your config): {} in {}.{}", key, repo, entity);
    }
    return this;
  }

  public StandardEvaluationContext getContext() {
    return inner.context;
  }

  public void setContext(StandardEvaluationContext context) {
    inner.context = context;
    context.setRootObject(this);
  }

  public void recycleParseContext(ThreadLocal<StandardEvaluationContext> contexts) {
    inner.context = null;
    contexts.remove();
  }

  public HashMap<String, Object> getFields() {
    return fields;
  }

  public HashMap<String, Object> getExtra() {
    return extra;
  }

  public Object getField(String key) {
    Assert.isTrue(fields.containsKey(key), fields.toString() + "[No such field]: " + key);
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
    if (inner.hasExtra) {
      logger.warn("Multiple insert by query, not supported for mysql output channel: old query will be override");
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
        "syncByQuery=" + syncByQuery +
        ", inner=" + inner +
        ", fields=" + fields +
        ", extra=" + extra +
        ", repo='" + repo + '\'' +
        ", table='" + entity + '\'' +
        ", id=" + id +
        ", primaryKeyName='" + primaryKeyName + '\'' +
        '}';
  }
}
