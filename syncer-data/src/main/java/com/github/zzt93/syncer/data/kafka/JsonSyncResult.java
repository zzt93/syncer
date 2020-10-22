package com.github.zzt93.syncer.data.kafka;

import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.data.SyncMeta;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import lombok.Getter;
import lombok.ToString;

import java.lang.reflect.Type;

/**
 * @author zzt
 */
@ToString(callSuper = true)
@Getter
public class JsonSyncResult extends SyncMeta {

  private static final Gson gson = new Gson();
  private JsonObject fields;
  private JsonObject extras;
  private JsonObject before;

  public JsonSyncResult(JsonObject fields, JsonObject extras, JsonObject before, SimpleEventType eventType, String repo, String entity, Object id, String primaryKeyName) {
    this.fields = fields;
    this.extras = extras;
    this.before = before;
    this.eventType = eventType;
    this.repo = repo;
    this.entity = entity;
    this.id = id;
    this.primaryKeyName = primaryKeyName;
  }

  private static Long getLong(Object res) {
    if (res != null) {
      try {
        return Long.parseLong((String) res);
      } catch (ClassCastException e) {
        if (res instanceof Double) { // for backward compatible
          return ((Double) res).longValue();
        }
      }
    }
    return null;
  }

  public Long getIdAsLong() {
    return getLong(getId());
  }

  public <T> T getFields(Class<T> tClass) {
    fields.add(primaryKeyName, new JsonPrimitive(id.toString()));
    return gson.fromJson(fields, tClass);
  }

  public <T> T getFields(Type typeOfT) throws JsonSyntaxException {
    fields.add(primaryKeyName, new JsonPrimitive(id.toString()));
    return gson.fromJson(fields, typeOfT);
  }

  public <T> T getExtras(Type typeOfT) throws JsonSyntaxException {
    return gson.fromJson(extras, typeOfT);
  }

  public <T> T getExtras(Class<T> tClass) {
    return gson.fromJson(extras, tClass);
  }

  public <T> T getBefore(Type typeOfT) throws JsonSyntaxException {
    before.add(primaryKeyName, new JsonPrimitive(id.toString()));
    return gson.fromJson(before, typeOfT);
  }

  public <T> T getBefore(Class<T> tClass) {
    before.add(primaryKeyName, new JsonPrimitive(id.toString()));
    return gson.fromJson(before, tClass);
  }

}
