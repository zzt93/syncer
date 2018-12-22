package com.github.zzt93.syncer.common.data;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.zzt93.syncer.common.IdGenerator;
import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.data.SyncResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zzt
 */
public class SyncData implements com.github.zzt93.syncer.data.SyncData, Serializable {

  private final transient Logger logger = LoggerFactory.getLogger(SyncData.class);
  private final Meta inner;
  private SyncByQuery syncByQuery;
  /**
   * sync result data fields
   */
  private SyncResult result = new SyncResult();

  public SyncData(String eventId, int ordinal, String database, String entity, String primaryKeyName,
                  Object id, Map<String, Object> row, EventType type) {
    inner = new Meta(eventId, ordinal, -1, null);

    result.setEventType(toSimpleEvent(type));
    setPrimaryKeyName(primaryKeyName);
    if (id != null) {
      setId(id);
    } else {
      logger.error("{} without primary key", type);
    }
    setRepo(database);
    setEntity(entity);
    getFields().putAll(row);
  }

  public SyncData(SyncData syncData, int offset) {
    inner = new Meta(syncData.getEventId(), syncData.inner.ordinal, offset,
        syncData.getSourceIdentifier());
    inner.context = EvaluationFactory.context();
    inner.context.setRootObject(this);
    result.setEventType(syncData.getType());
  }


  @Override
  public Object getId() {
    return result.getId();
  }

  @Override
  public SyncData setId(Object id) {
    result.setId(id);
    return this;
  }

  @Override
  public String getEntity() {
    return result.getEntity();
  }

  @Override
  public SyncData setEntity(String entity) {
    result.setEntity(entity);
    return this;
  }

  @Override
  public boolean isWrite() {
    return result.getEventType() == SimpleEventType.WRITE;
  }

  @Override
  public boolean isUpdate() {
    return result.getEventType() == SimpleEventType.UPDATE;
  }

  @Override
  public boolean isDelete() {
    return result.getEventType() == SimpleEventType.DELETE;
  }

  @Override
  public boolean toWrite() {
    return updateType(SimpleEventType.WRITE);
  }

  @Override
  public boolean toUpdate() {
    return updateType(SimpleEventType.UPDATE);
  }

  @Override
  public boolean toDelete() {
    return updateType(SimpleEventType.DELETE);
  }

  private boolean updateType(SimpleEventType type) {
    boolean res = result.getEventType() == type;
    result.setEventType(type);
    return res;
  }


  @Override
  public String getRepo() {
    return result.getRepo();
  }

  @Override
  public SyncData setRepo(String repo) {
    result.setRepo(repo);
    return this;
  }

  @Override
  public Object getExtra(String key) {
    return result.getExtra(key);
  }

  @Override
  public SyncData addExtra(String key, Object value) {
    result.addExtra(key, value);
    return this;
  }

  public SyncData addField(String key, Object value) {
    if (value == null) {
      logger.warn("Adding column({}) with null, discarded", key);
    }
    getFields().put(key, value);
    return this;
  }

  public SyncData renameField(String oldKey, String newKey) {
    if (containField(oldKey)) {
      getFields().put(newKey, getFields().get(oldKey));
      getFields().remove(oldKey);
    } else {
      logger.warn("No such field name (maybe filtered out): `{}` in `{}`.`{}`", oldKey, getRepo(), getEntity());
    }
    return this;
  }

  public SyncData removeField(String key) {
    getFields().remove(key);
    return this;
  }

  public boolean removePrimaryKey() {
    return getPrimaryKeyName() != null && getFields().remove(getPrimaryKeyName()) != null;
  }

  public SyncData removeFields(String... keys) {
    for (String colName : keys) {
      getFields().remove(colName);
    }
    return this;
  }

  public boolean containField(String key) {
    return result.containField(key);
  }

  public SyncData updateField(String key, Object value) {
    if (containField(key)) {
      if (value != null) {
        getFields().put(key, value);
      } else {
        logger.warn("update field[{}] with null", key);
      }
    } else {
      logger.warn("No such field name (check your config): {} in {}.{}", key, getRepo(), getEntity());
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

  @Override
  public HashMap<String, Object> getFields() {
    return result.getFields();
  }

  @Override
  public HashMap<String, Object> getExtra() {
    return result.getExtra();
  }

  public Object getField(String key) {
    if (result.getField(key) == null) {
      logger.info("[No such field]: {}, {}", key, getFields().toString());
      return null;
    }
    return result.getField(key);
  }

  public String getEventId() {
    return inner.eventId;
  }

  public String getDataId() {
    return inner.dataId;
  }

  public String getSourceIdentifier() {
    return inner.connectionIdentifier;
  }

  public SyncData setSourceIdentifier(String identifier) {
    inner.connectionIdentifier = identifier;
    return this;
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

  public String getPrimaryKeyName() {
    return result.getPrimaryKeyName();
  }

  public void setPrimaryKeyName(String primaryKeyName) {
    result.setPrimaryKeyName(primaryKeyName);
  }

  public SimpleEventType getType() {
    return result.getType();
  }

  @Override
  public String toString() {
    return "SyncData{" +
        "inner=" + inner +
        ", syncByQuery=" + syncByQuery +
        ", result=" + result +
        '}';
  }

  public SyncResult getResult() {
    return result;
  }

  public static SimpleEventType toSimpleEvent(EventType type) {
    if (EventType.isDelete(type)) {
      return SimpleEventType.DELETE;
    }
    if (EventType.isUpdate(type)) {
      return SimpleEventType.UPDATE;
    }
    if (EventType.isWrite(type)) {
      return SimpleEventType.WRITE;
    }
    throw new IllegalArgumentException("Unknown " + type);
  }

  private static class Meta {
    private final String eventId;
    private final String dataId;
    private final int ordinal;
    private transient StandardEvaluationContext context;
    private boolean hasExtra = false;
    private String connectionIdentifier;

    Meta(String eventId, int ordinal, int offset, String connectionIdentifier) {
      this.eventId = eventId;
      this.connectionIdentifier = connectionIdentifier;
      if (offset < 0) {
        dataId = IdGenerator.fromEventId(eventId, ordinal);
      } else {
        dataId = IdGenerator.fromEventId(eventId, ordinal, offset);
      }
      this.ordinal = ordinal;
    }

    @Override
    public String toString() {
      return "Meta{" +
          "eventId='" + eventId + '\'' +
          ", dataId='" + dataId + '\'' +
          ", ordinal=" + ordinal +
          ", context=" + context +
          ", hasExtra=" + hasExtra +
          ", connectionIdentifier='" + connectionIdentifier + '\'' +
          '}';
    }

  }
}
