package com.github.zzt93.syncer.data;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import java.util.Map;

/**
 * @author zzt
 */
public class SyncUtil {

  private static final Gson gson = new Gson();

  public static String toJson(Object o) {
    if (o == null) {
      return null;
    }
    return gson.toJson(o);
  }

  public static <T> T fromJson(String json, Class<T> clazz) {
    if (json == null) {
      return null;
    }
    try {
      return gson.fromJson(json, clazz);
    } catch (JsonSyntaxException e) {
      return null;
    }
  }

  public static Map fromJson(String json) {
    return fromJson(json, Map.class);
  }

}
