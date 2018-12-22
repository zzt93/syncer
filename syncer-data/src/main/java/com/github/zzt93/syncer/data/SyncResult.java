package com.github.zzt93.syncer.data;

import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * @author zzt
 */
public class SyncResult {

  /**
   * {@link #fields} have to use `LinkedHashMap` to be in order to support multiple dependent extraQuery
   */
  private final HashMap<String, Object> fields = new LinkedHashMap<>();
  private final HashMap<String, Object> extra = new HashMap<>();

  private SimpleEventType eventType;
  private String repo;
  private String entity;
  /**
   * entity primary key
   */
  private Object id;
  private String primaryKeyName;

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

  public SimpleEventType getType() {
    return getEventType();
  }

  public boolean containField(String key) {
    return fields.containsKey(key);
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

  public String getPrimaryKeyName() {
    return primaryKeyName;
  }

  public SyncResult setRepo(String repo) {
    this.repo = repo;
    return this;
  }

  public SyncResult setId(Object id) {
    this.id = id;
    return this;
  }

  public void setPrimaryKeyName(String primaryKeyName) {
    this.primaryKeyName = primaryKeyName;
  }

  @Override
  public String toString() {
    return "SyncData{" +
        "fields=" + fields +
        ", extra=" + extra +
        ", repo='" + repo + '\'' +
        ", entity='" + entity + '\'' +
        ", id=" + id +
        ", primaryKeyName='" + primaryKeyName + '\'' +
        '}';
  }

  public void addExtra(String key, Object value) {
    extra.put(key, value);
  }

  public Object getExtra(String key) {
    return extra.get(key);
  }

}
