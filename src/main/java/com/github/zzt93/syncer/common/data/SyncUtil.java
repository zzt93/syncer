package com.github.zzt93.syncer.common.data;

import com.google.gson.Gson;

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

  public static Object fromJson(String json, Class<?> clazz) {
    if (json == null) {
      return null;
    }
    return gson.fromJson(json, clazz);
  }

}
