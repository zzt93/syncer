package com.github.zzt93.syncer.data;

import com.google.gson.*;

import java.lang.reflect.Type;

/**
 * @author zzt
 */
public enum SimpleEventType {
  WRITE() {
    @Override
    public String abbr() {
      return "w";
    }
  }, UPDATE {
    @Override
    public String abbr() {
      return "u";
    }
  }, DELETE {
    @Override
    public String abbr() {
      return "d";
    }
  };

  public abstract String abbr();

  public static SimpleEventType get(int ordinal) {
    return values()[ordinal];
  }

  public static final JsonSerializer<SimpleEventType> defaultSerializer = (SimpleEventType src, Type typeOfSrc, JsonSerializationContext context) -> new JsonPrimitive(src.ordinal());
  public static final JsonDeserializer<SimpleEventType> defaultDeserializer = (JsonElement json, Type typeOfT, JsonDeserializationContext context) -> {
    try {
      return get(json.getAsInt());
    } catch (Exception e) { // for backward compatibility
      return valueOf(json.getAsString());
    }
  };

}
