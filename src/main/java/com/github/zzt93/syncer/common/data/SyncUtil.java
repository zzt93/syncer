package com.github.zzt93.syncer.common.data;

import com.github.zzt93.syncer.consumer.filter.impl.Switch;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class SyncUtil {
  private static final Logger logger = LoggerFactory.getLogger(Switch.class);

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
    try {
      return gson.fromJson(json, clazz);
    } catch (JsonSyntaxException e) {
      logger.error("Fail to parse json string {} to {}", json, clazz);
      return null;
    }
  }

}
