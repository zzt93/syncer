package com.github.zzt93.syncer.common.data;

import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.data.es.Filter;
import com.github.zzt93.syncer.producer.dispatch.NamedChange;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

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
  private final SyncInfo inner;
  /**
   * sync result data fields
   */
  private SyncResult result;

  public SyncData(DataId dataId, SimpleEventType type, String database, String entity, String primaryKeyName,
                  Object id, NamedChange row) {
    inner = innerSyncData(dataId, type, database, entity, primaryKeyName, id, row.getFull(), row.getBeforeFull(), row.getUpdated());
  }

  private SyncData(DataId dataId, SimpleEventType type, String database, String entity, String primaryKeyName, Object id,
                             HashMap<String, Object> full, HashMap<String, Object> beforeFull, Set<String> updated){
    inner = innerSyncData(dataId, type, database, entity, primaryKeyName, id, full, beforeFull, updated);
  }

  private SyncInfo innerSyncData(DataId dataId, SimpleEventType type, String database, String entity, String primaryKeyName, Object id,
																 Map<String, Object> full, HashMap<String, Object> beforeFull, Set<String> updated) {
    result = new SyncResult(type, database, entity, primaryKeyName, id, full);

    if (isUpdate()) {
      result.setBefore(beforeFull);
    }

    return new SyncInfo(dataId, null, updated);
  }

  private SyncData(SyncData syncData, int offset) {
    inner = new SyncInfo(((BinlogDataId) syncData.inner.dataId).copyAndSetOffset(offset), syncData.getSourceIdentifier(), null);
    result = new SyncResult();
    result.setEventType(syncData.getType());
    result.setRepo(syncData.getRepo());
    result.setEntity(syncData.getEntity());
    result.setPrimaryKeyName(syncData.getPrimaryKeyName());
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
    return null;
  }

  @Override
  public com.github.zzt93.syncer.data.SyncData copyMeta() {
    return new SyncData(this, inner.copy++);
  }

  /**
   *
   * @see #SyncData(DataId, SimpleEventType, String, String, String, Object, NamedChange)
   */
  @Override
  public boolean updated() {
    return getUpdated() != null;
  }

  @Override
  public boolean updated(String key) {
    return updated() && getUpdated().contains(key);
  }

  @Override
  public Set<String> getUpdated() {
    return inner.updated;
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
  public SyncData es(String index, String type) {
  	inner.output.es(index, type);
    return this;
  }

  @Override
  public SyncData es(String index, String type, String id) {
    inner.output.es(index, type);
    return setId(id);
  }

  public SyncData es(Supplier<String> index, Supplier<String> type) {
    return this;
  }

  @Override
  public SyncData mysql(String db, String table) {
  	inner.output.db(db, table);
    return this;
  }

  @Override
  public SyncData mysql(String db, String table, Object id) {
    inner.output.db(db, table);
    return setId(id);
  }

  public SyncData mysql(Supplier<String> db, Supplier<String> table) {
    return this;
  }

  @Override
  public SyncData kafka(String topic) {
  	inner.output.kafka(topic);
    return this;
  }

  @Override
  public SyncData kafka(String topic, Object partitionKey) {
    inner.output.kafka(topic);
    return setPartitionKey(partitionKey);
  }

  public SyncData kafka(Supplier<String> topic) {
    return this;
  }

  @Override
  public SyncData addExtra(String key, Object value) {
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

  @Override
  public HashMap<String, Object> getFields() {
    return result.getFields();
  }

  @Override
  public HashMap<String, Object> getExtras() {
    return null;
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
    if (getSyncByQuery() == null) {
      return null;
    }
    return getSyncByQuery().getSyncBy();
  }

  /**
   * update/delete by query
   */
  public SyncByQuery syncByQuery() {
    if (getSyncByQuery() == null) {
      setSyncByQuery(new SyncByQuery(this));
    }
    return getSyncByQuery();
  }

  public ESScriptUpdate esScriptUpdate() {
    if (getEsScriptUpdate() == null) {
      setEsScriptUpdate(new ESScriptUpdate(this));
    }
    return getEsScriptUpdate();
  }

  @Override
  public ESScriptUpdate esScriptUpdate(String script, Map<String, Object> params) {
    if (getEsScriptUpdate() == null) {
      setEsScriptUpdate(new ESScriptUpdate(this, script, params));
    }
    return getEsScriptUpdate();
  }

  @Override
  public ESScriptUpdate esScriptUpdate(Filter docFilter) {
    if (getEsScriptUpdate() == null) {
      setEsScriptUpdate(new ESScriptUpdate(this, docFilter));
    }
    return getEsScriptUpdate();
  }

  public ESScriptUpdate getEsScriptUpdate() {
    return inner.esScriptUpdate;
  }

  public ExtraQuery extraQuery(String indexName, String typeName) {
    if (getExtraQueryContext() == null) {
      setExtraQueryContext(new ExtraQueryContext());
    }
    return getExtraQueryContext().add(new ExtraQuery(this).setIndexName(indexName).setTypeName(typeName));
  }

  public ExtraQueryContext getExtraQueryContext() {
    return inner.extraQueryContext;
  }


  public boolean hasExtraQuery() {
    return getExtraQueryContext() != null;
  }

  public String getPrimaryKeyName() {
    return result.getPrimaryKeyName();
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

  public Long getPartitionKey() {
    Object o;
    if (inner.partitionKey == null) {
      o = getPartitionField() == null || getField(getPartitionField()) == null ? getId() : getField(getPartitionField());
    } else {
      o = inner.partitionKey;
    }
    if (o != null) {
      return Math.abs(o instanceof Long ? ((Long) o) : o.hashCode());
    }
    logger.warn("No primary key to use and no partitionId configured, {}", this);
    return 0L;
  }

  public SyncData setPartitionField(String fieldName) {
    this.inner.partitionField = fieldName;
    return this;
  }

  private SyncData setPartitionKey(Object partitionKey) {
    this.inner.partitionKey = partitionKey;
    return this;
  }

  private String getPartitionField() {
    return inner.partitionField;
  }

	private SyncByQuery getSyncByQuery() {
		return inner.syncByQuery;
	}

	private void setSyncByQuery(SyncByQuery syncByQuery) {
		this.inner.syncByQuery = syncByQuery;
	}

	private void setEsScriptUpdate(ESScriptUpdate esScriptUpdate) {
		this.inner.esScriptUpdate = esScriptUpdate;
	}

	private void setExtraQueryContext(ExtraQueryContext extraQueryContext) {
		this.inner.extraQueryContext = extraQueryContext;
	}

  public String getEsId() {
    return getId() == null ? null : getId().toString();
  }

  public String getEsIndex() {
    return inner.output.getEsIndex(getRepo());
  }

  public String getEsType() {
    return inner.output.getEsType(getEntity());
  }

  public String getDb() {
    return inner.output.getDb(getRepo());
  }

  public String getTable() {
    return inner.output.getTable(getEntity());
  }

  public String getDbId() {
    return getId() == null ? null : getId().toString();
  }

  public String getKafkaTopic() {
    return inner.output.getKafkaTopic(getRepo() + getEntity());
  }

  /**
   * used in sync process, not exposed by default
   */
	private static class SyncInfo {
    private final DataId dataId;
    private String connectionIdentifier;

		private ExtraQueryContext extraQueryContext;
		private SyncByQuery syncByQuery;
		private ESScriptUpdate esScriptUpdate;
		private Set<String> updated;
		private byte copy;

		private String partitionField;
		private Object partitionKey;
		private OutputInfo output = new OutputInfo();

		SyncInfo(DataId dataId, String connectionIdentifier, Set<String> updated) {
      this.dataId = dataId;
      this.connectionIdentifier = connectionIdentifier;
      this.updated = updated;
    }

    @Override
    public String toString() {
      return "Meta{" +
          "dataId=" + dataId +
          ", connectionIdentifier='" + connectionIdentifier + '\'' +
					", syncByQuery=" + syncByQuery +
					", esScriptUpdate=" + esScriptUpdate +
					", updated=" + updated +
					", partitionField=" + partitionField +
					", copy=" + copy +
					", extraQueryContext=" + extraQueryContext +
          '}';
    }

	}

  private static class OutputInfo {
	  private String esIndex;
	  private String esType;
	  private String db;
	  private String table;
	  private String kafkaTopic;

		public void es(String index, String type) {
			esIndex = index;
			esType = type;
		}

		public void db(String db, String table) {
			this.db = db;
			this.table = table;
		}

		public void kafka(String topic) {
			this.kafkaTopic = topic;
		}

    String getEsIndex(String repo) {
      return esIndex == null ? repo : esIndex;
    }

    String getEsType(String entity) {
      return esType == null ? entity : esType;
    }

    String getDb(String repo) {
      return db == null ? repo : db;
    }

    String getTable(String entity) {
      return table == null ? entity : table;
    }

    String getKafkaTopic(String topic) {
      return kafkaTopic == null ? topic : kafkaTopic;
    }
  }
}
