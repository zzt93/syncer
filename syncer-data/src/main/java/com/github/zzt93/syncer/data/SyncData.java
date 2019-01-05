package com.github.zzt93.syncer.data;

import java.util.HashMap;

/**
 * Config operation interface
 * @author zzt
 */
public interface SyncData {

  Object getId();

  SyncData setId(Object id);

  String getEntity();

  boolean isWrite();

  boolean isUpdate();

  boolean isDelete();

  boolean toWrite();

  boolean toUpdate();

  boolean toDelete();

  SyncData setEntity(String entity);

  String getRepo();

  SyncData setRepo(String repo);

  SimpleEventType getType();

  SyncData addExtra(String key, Object value);

  SyncData addField(String key, Object value);

  SyncData renameField(String oldKey, String newKey);

  SyncData removeField(String key);

  boolean removePrimaryKey();

  SyncData removeFields(String... keys);

  boolean containField(String key);

  SyncData updateField(String key, Object value);

  HashMap<String, Object> getFields();

  HashMap<String, Object> getExtra();

  Object getField(String key);

  String getEventId();

  String getDataId();

  SyncData setSourceIdentifier(String identifier);

  String getSourceIdentifier();

  HashMap<String, Object> getSyncBy();

  /**
   * update/delete by query
   */
  SyncByQuery syncByQuery();

  ExtraQuery extraQuery(String indexName, String typeName);

  boolean hasExtra();

  Object getExtra(String key);

  @Override
  String toString();
}
