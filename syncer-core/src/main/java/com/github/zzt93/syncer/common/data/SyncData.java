package com.github.zzt93.syncer.common.data;

import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.data.es.Filter;
import com.github.zzt93.syncer.producer.dispatch.NamedChange;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.TypedValue;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Output channel parse order (MySQL & ES):
 * <ul>
 *   <li>extraQuery</li>
 *   <li>fields (support extraQuery), id, repo, entity</li>
 *   <li>syncByQuery</li>
 *   <li>EsScript (support extraQuery)</li>
 * </ul>
 * @author zzt
 */
@ToString
public class SyncData implements com.github.zzt93.syncer.data.SyncData, Serializable {

  private static final transient Logger logger = LoggerFactory.getLogger(SyncData.class);
  private final Meta inner;
  private SyncByQuery syncByQuery;
  private ESScriptUpdate esScriptUpdate;
  /**
   * sync result data fields
   */
  private SyncResult result;
  private Set<String> updated;
  private String partitionField;

  public SyncData(DataId dataId, SimpleEventType type, String database, String entity, String primaryKeyName,
                  Object id, NamedChange row) {
    inner = innerSyncData(dataId, type, database, entity, primaryKeyName, id, row.getFull(), row.getBeforeFull(), row.getUpdated());
  }

  private SyncData(DataId dataId, SimpleEventType type, String database, String entity, String primaryKeyName, Object id,
                             HashMap<String, Object> full, HashMap<String, Object> beforeFull, Set<String> updated){
    inner = innerSyncData(dataId, type, database, entity, primaryKeyName, id, full, beforeFull, updated);
  }

  private Meta innerSyncData(DataId dataId, SimpleEventType type, String database, String entity, String primaryKeyName, Object id,
                             HashMap<String, Object> full, HashMap<String, Object> beforeFull, Set<String> updated) {
    result = new SyncResult(type, database, entity, primaryKeyName, id, full);

    if (isUpdate()) {
      result.setBefore(beforeFull);
      this.updated = updated;
    }

    return new Meta(dataId, null);
  }

  public SyncData(SyncData syncData, int offset) {
    inner = new Meta(((BinlogDataId) syncData.inner.dataId).copyAndSetOffset(offset), syncData.getSourceIdentifier());
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
  public SyncData toWrite() {
    return updateType(SimpleEventType.WRITE);
  }

  @Override
  public SyncData toUpdate() {
    return updateType(SimpleEventType.UPDATE);
  }

  @Override
  public SyncData toDelete() {
    return updateType(SimpleEventType.DELETE);
  }

  private SyncData updateType(SimpleEventType type) {
    result.setEventType(type);
    return this;
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
   * @see #SyncData(DataId, SimpleEventType, String, String, String, Object, NamedChange)
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
  public Set<String> getUpdated() {
    return updated;
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

  @Override
  public SyncData setFieldNull(String key) {
    getFields().put(key, null);
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

  public Object removeField(String key) {
    return getFields().remove(key);
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
//    contexts.remove();
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

  @Override
  public Long getFieldAsLong(String key) {
    Object res = getField(key);
    if (res != null) {
      return (long) res;
    }
    return null;
  }

  @Override
  public Integer getFieldAInt(String key) {
    Object res = getField(key);
    if (res != null) {
      return (int) res;
    }
    return null;
  }

  public String getEventId() {
    return inner.dataId.eventId();
  }

  public DataId getDataId() {
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
      syncByQuery = new SyncByQuery(this);
    }
    return syncByQuery;
  }

  public ESScriptUpdate esScriptUpdate() {
    if (esScriptUpdate == null) {
      esScriptUpdate = new ESScriptUpdate(this);
    }
    return esScriptUpdate;
  }

  @Override
  public ESScriptUpdate esScriptUpdate(String script, Map<String, Object> params) {
    if (esScriptUpdate == null) {
      esScriptUpdate = new ESScriptUpdate(this, script, params);
    }
    return esScriptUpdate;
  }

  @Override
  public ESScriptUpdate esScriptUpdate(Filter docFilter) {
    if (esScriptUpdate == null) {
      esScriptUpdate = new ESScriptUpdate(this, docFilter);
    }
    return esScriptUpdate;
  }

  public ESScriptUpdate getEsScriptUpdate() {
    return esScriptUpdate;
  }

  public ExtraQuery extraQuery(String indexName, String typeName) {
    if (extraQueryContext == null) {
      extraQueryContext = new ExtraQueryContext();
    }
    return extraQueryContext.add(new ExtraQuery(this).setIndexName(indexName).setTypeName(typeName));
  }

  public ExtraQueryContext getExtraQueryContext() {
    return extraQueryContext;
  }

  private ExtraQueryContext extraQueryContext;

  public boolean hasExtra() {
    return extraQueryContext != null;
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

  public SyncResult getResult() {
    return result;
  }

  void syncByForeignKey() {
    result.setId(null);
  }

  public SyncData copy() {
    HashMap<String, Object> before = getBefore() != null ? new HashMap<>(getBefore()) : null;
    Set<String> updated = getUpdated() != null ? new HashSet<>(getUpdated()) : null;
    return new SyncData(getDataId(), getType(), getRepo(), getEntity(), getPrimaryKeyName(), getId(), new HashMap<>(getFields()), before, updated);
  }

  public Long getPartitionId() {
    Object o = partitionField == null || getField(partitionField) == null ? getId() : getField(partitionField);
    if (o != null) {
      return Math.abs(o instanceof Long ? ((Long) o) : o.hashCode());
    }
    logger.warn("No primary key to use and no partitionId configured, {}", this);
    return 0L;
  }

  public void setPartitionField(String fieldName) {
    this.partitionField = fieldName;
  }

	private static class Meta {
    private final DataId dataId;
    private transient StandardEvaluationContext context;
    private String connectionIdentifier;

    Meta(DataId dataId, String connectionIdentifier) {
      this.dataId = dataId;
      this.connectionIdentifier = connectionIdentifier;
    }

    @Override
    public String toString() {
      TypedValue rootObject = context != null ? context.getRootObject() : TypedValue.NULL;
      return "Meta{" +
          "dataId=" + dataId +
          ", context=" + (rootObject.getValue() != null ? ((SyncData) rootObject.getValue()).inner == this : null) +
          ", connectionIdentifier='" + connectionIdentifier + '\'' +
          '}';
    }
  }
}
