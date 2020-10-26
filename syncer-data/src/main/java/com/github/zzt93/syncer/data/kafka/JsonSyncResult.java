package com.github.zzt93.syncer.data.kafka;

import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.data.SyncMeta;
import com.github.zzt93.syncer.data.util.SyncUtil;
import com.google.gson.*;
import lombok.Getter;
import lombok.ToString;

import java.lang.reflect.Type;
import java.util.Map;

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
    JsonObject tmp = copyAndConvert(fields);
    tmp.add(SyncUtil.underscoreToCamel(primaryKeyName), getValue());
    return gson.fromJson(tmp, tClass);
  }

  private JsonPrimitive getValue() {
    if (!(id instanceof Double)) {
      return new JsonPrimitive(id.toString());
    }
    return new JsonPrimitive((Double) id);  // for backward compatible
  }

  public <T> T getFields(Type typeOfT) throws JsonSyntaxException {
    JsonObject tmp = copyAndConvert(fields);
    tmp.add(SyncUtil.underscoreToCamel(primaryKeyName), getValue());
    return gson.fromJson(tmp, typeOfT);
  }

  public <T> T getExtras(Type typeOfT) throws JsonSyntaxException {
    return gson.fromJson(copyAndConvert(extras), typeOfT);
  }

  public <T> T getExtras(Class<T> tClass) {
    return gson.fromJson(copyAndConvert(extras), tClass);
  }

  public <T> T getBefore(Type typeOfT) throws JsonSyntaxException {
    JsonObject tmp = copyAndConvert(before);
    tmp.add(primaryKeyName, getValue());
    return gson.fromJson(tmp, typeOfT);
  }

  public <T> T getBefore(Class<T> tClass) {
    JsonObject tmp = copyAndConvert(before);
    tmp.add(primaryKeyName, getValue());
    return gson.fromJson(tmp, tClass);
  }

  private JsonObject copyAndConvert(JsonObject jsonObject) {
    JsonObject tmp = new JsonObject();
    for (Map.Entry<String, JsonElement> stringJsonElementEntry : jsonObject.entrySet()) {
      tmp.add(SyncUtil.underscoreToCamel(stringJsonElementEntry.getKey()), stringJsonElementEntry.getValue());
    }
    return tmp;
  }

}
