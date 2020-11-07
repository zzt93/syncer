package com.github.zzt93.syncer.data;

import java.sql.Timestamp;
import java.util.Date;
import com.github.zzt93.syncer.data.es.Filter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Config operation interface
 * @author zzt
 */
public interface SyncData {

  Object getId();

  SyncData setId(Object id);

  String getEntity();

  boolean isWrite();

  /**
   * @deprecated {@link #updated()} might be better
   * @return whether this event type is {@link SimpleEventType}
   */
  boolean isUpdate();

  boolean isDelete();

  SyncData toWrite();

  SyncData toUpdate();

  SyncData toDelete();

  SyncData setEntity(String entity);

  String getRepo();

  SyncData setRepo(String repo);

  SimpleEventType getType();

  /**
   * Default use id as partitionKey, override with this method.
   *
   * @param fieldName name of field which is either int/long or String
   */
  void setPartitionField(String fieldName);

  SyncData addExtra(String key, Object value);

  SyncData addField(String key, Object value);

  SyncData setFieldNull(String key);

  SyncData renameField(String oldKey, String newKey);

  Object removeField(String key);

  SyncData removeFields(String... keys);

  boolean containField(String key);

  SyncData updateField(String key, Object value);

  HashMap<String, Object> getFields();

  HashMap<String, Object> getExtras();

  Object getField(String key);

  Long getFieldAsLong(String key);

  Integer getFieldAInt(String key);

  String getEventId();

  SyncData setSourceIdentifier(String identifier);

  String getSourceIdentifier();

  HashMap<String, Object> getSyncBy();

  /**
   * Call this method to update/delete/write by query
   * @return one instance for this SyncByQuery
   * @see SyncByQuery#syncBy(String, Object) add filter and will set id as null automatically
   */
  SyncByQuery syncByQuery();

  ESScriptUpdate esScriptUpdate();

  ESScriptUpdate esScriptUpdate(String script, Map<String, Object> params);

  /**
   * @param docFilter filter for query a ES doc and apply script, field value will be removed
   * @return ESScriptUpdate which contains script and filter
   */
  ESScriptUpdate esScriptUpdate(Filter docFilter);

  ExtraQuery extraQuery(String indexName, String typeName);

  Object getExtra(String key);

  /**
   * create a new instance with meta info copied
   * @param index ith copy of original data, should be different across invocation because it is used for logging
   * @return a new instance of {@link SyncData}
   */
  SyncData copyMeta(int index);

  /**
   * Determine whether updated according to {@link java.util.Objects#deepEquals(Object, Object)}
   * @return any interested column is updated in this event
   * @see java.util.Objects#deepEquals(Object, Object)
   */
  boolean updated();

  /**
   * @param key column name
   * @return whether this key is {{@link #updated()}} in this event
   * @see java.util.Objects#deepEquals(Object, Object)
   */
  boolean updated(String key);

  Set<String> getUpdated();

  Object getBefore(String key);

  /**
   * @return null if no before info (MongoDB update event has no before info now)
   */
  HashMap<String, Object> getBefore();

  @Override
  String toString();

  /**
   * @param key name for field which is byte[] in Java, which may come from blob type in db
   * @return this instance
   */
  default SyncData bytesToStr(String key) {
    Object value = getField(key);
    if (value != null) {
      updateField(key, new String((byte[]) value, java.nio.charset.StandardCharsets.UTF_8));
    }
    return this;
  }

  /**
   * @param key name for field which is byte[] in Java, which may come from blob type in db
   * @return this instance
   */
  default SyncData dateToTimestamp(String key) {
    Date value = (Date) getField(key);
    if (value != null) {
      updateField(key, new Timestamp(value.getTime()));
    }
    return this;
  }

  /**
   * @param key name for field which is java.sql.Timestamp in Java
   * @return this instance
   */
  default SyncData timestampToUnix(String key) {
    Timestamp value = (Timestamp) getField(key);
    if (value != null) {
      updateField(key, value.getTime());
    }
    return this;
  }

  /**
   * convert this mysql unsigned byte to positive in Java
   * @param key name for field which is int in Java
   * @return this
   */
  default SyncData recoverUnsignedByte(String key) {
    Object field = getField(key);
    if (field != null) {
      updateField(key, Byte.toUnsignedInt((byte)(int) field));
    }
    return this;
  }
}
