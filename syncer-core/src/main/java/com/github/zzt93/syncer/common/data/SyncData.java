package com.github.zzt93.syncer.common.data;

import com.github.zzt93.syncer.common.IdGenerator;
import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.data.SyncResult;
import com.github.zzt93.syncer.producer.dispatch.NamedChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;

/**
 * @author zzt
 */
public class SyncData implements com.github.zzt93.syncer.data.SyncData, Serializable {

  private static final transient Logger logger = LoggerFactory.getLogger(SyncData.class);
  private final Meta inner;
  private SyncByQuery syncByQuery;
  /**
   * sync result data fields
   */
  private SyncResult result;
  private Set<String> updated;

  public SyncData(String eventId, int ordinal, SimpleEventType type, String database, String entity, String primaryKeyName,
                  Object id, NamedChange row) {
    inner = new Meta(eventId, ordinal, -1, null);
    result = new SyncResult(row.getFull());

    setPrimaryKeyName(primaryKeyName);
    setId(id);
    setEntity(entity);
    setRepo(database);
    result.setEventType(type);
    if (isUpdate()) {
      result.setBefore(row.getBeforeFull());
      updated = row.getUpdated();
    }
  }

  public SyncData(SyncData syncData, int offset) {
    inner = new Meta(syncData.getEventId(), syncData.inner.ordinal, offset,
        syncData.getSourceIdentifier());
    inner.context = EvaluationFactory.context();
    inner.context.setRootObject(this);
    result = new SyncResult();
    result.setEventType(syncData.getType());
  }


  @Override
  public Object getId() {
    return result.getId();
  }

  @Override
  public SyncData setId(Object id) {
    if (id != null) {
      result.setId(id);
    } else {
      logger.warn("Update primary key with null");
    }
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
  public com.github.zzt93.syncer.data.SyncData copyMeta(int index) {
    return new SyncData(this, index);
  }

  /**
   *
   * @see #SyncData(String, int, SimpleEventType, String, String, String, Object, NamedChange)
   */
  @Override
  public boolean updated() {
    return updated != null;
  }

  @Override
  public boolean updated(String key) {
    return updated() && updated.contains(key);
  }

  @Override
  public Object getBefore(String key) {
    return result.getBefore(key);
  }

  @Override
  public HashMap<String, Object> getBefore() {
    return result.getBefore();
  }

  @Override
  public SyncData addExtra(String key, Object value) {
    result.getExtras().put(key, value);
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
    return result.getFields().containsKey(key);
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
  public HashMap<String, Object> getExtras() {
    return result.getExtras();
  }

  public Object getField(String key) {
    if (result.getFields().get(key) == null) {
      logger.info("[No such field]: {}, {}", key, getFields().toString());
      return null;
    }
    return result.getFields().get(key);
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

  private String getPrimaryKeyName() {
    return result.getPrimaryKeyName();
  }

  private void setPrimaryKeyName(String primaryKeyName) {
    result.setPrimaryKeyName(primaryKeyName);
  }

  public SimpleEventType getType() {
    return result.getEventType();
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

  void syncByForeignKey() {
    result.setId(null);
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
