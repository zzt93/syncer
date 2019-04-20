package com.github.zzt93.syncer.data;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author zzt
 */
public class SyncResult {

  /**
   * {@link #fields} have to use `LinkedHashMap` to be in order to support multiple dependent {@link ExtraQuery}
   */
  private LinkedHashMap<String, Object> fields;
  private LinkedHashMap<String, Object> extras;
  private HashMap<String, Object> before;

  private SimpleEventType eventType;
  private String repo;
  private String entity;
  /**
   * entity primary key
   */
  private Object id;
  private String primaryKeyName;

  public SyncResult() {
    fields = new LinkedHashMap<>();
  }

  public SyncResult(Map<String, Object> row) {
    fields = new LinkedHashMap<>(row);
  }

  public Object getId() {
    return id;
  }

  public String getEntity() {
    return entity;
  }

  public void setEntity(String entity) {
    this.entity = entity;
  }

  public void setEventType(SimpleEventType eventType) {
    this.eventType = eventType;
  }

  public SimpleEventType getEventType() {
    return eventType;
  }

  public String getRepo() {
    return repo;
  }

  public HashMap<String, Object> getFields() {
    return fields;
  }

  public Object getExtra(String key) {
    return extras != null ? extras.get(key) : null;
  }

  public Object getBefore(String key) {
    return before != null ? before.get(key) : null;
  }

  /**
   * Not thread-safe, should not be invoked by multiple thread
   */
  public HashMap<String, Object> getExtras() {
    if (extras == null) {
      extras = new LinkedHashMap<>();
    }
    return extras;
  }

  public HashMap<String, Object> getBefore() {
    return before;
  }

  public String getPrimaryKeyName() {
    return primaryKeyName;
  }

  public void setRepo(String repo) {
    this.repo = repo;
  }

  public void setId(Object id) {
    this.id = id;
  }

  public void setPrimaryKeyName(String primaryKeyName) {
    this.primaryKeyName = primaryKeyName;
  }

  public void setBefore(HashMap<String, Object> before) {
    this.before = before;
  }

  @Override
  public String toString() {
    return "SyncResult{" +
        "fields=" + fields +
        ", extras=" + extras +
        ", eventType=" + eventType +
        ", repo='" + repo + '\'' +
        ", entity='" + entity + '\'' +
        ", id=" + id +
        ", primaryKeyName='" + primaryKeyName + '\'' +
        '}';
  }

}
