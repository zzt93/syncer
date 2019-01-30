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

  public HashMap<String, Object> getFields() {
    return fields;
  }

  public HashMap<String, Object> getExtra() {
    return extra;
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

  @Override
  public String toString() {
    return "SyncResult{" +
        "fields=" + fields +
        ", extra=" + extra +
        ", eventType=" + eventType +
        ", repo='" + repo + '\'' +
        ", entity='" + entity + '\'' +
        ", id=" + id +
        ", primaryKeyName='" + primaryKeyName + '\'' +
        '}';
  }
}
