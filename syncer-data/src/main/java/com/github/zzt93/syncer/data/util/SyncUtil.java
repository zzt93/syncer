package com.github.zzt93.syncer.data.util;

import com.github.zzt93.syncer.data.SyncData;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


/**
 * @author zzt
 */
public class SyncUtil {
  private static final Logger logger = LoggerFactory.getLogger(SyncUtil.class);

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
      logger.error("Fail to parse json string {} to {}", json, clazz);
      return null;
    }
  }

  public static <T> T fromJson(String json, TypeToken<T> token) {
    if (json == null) {
      return null;
    }
    try {
      return gson.fromJson(json, token.getType());
    } catch (JsonSyntaxException e) {
      logger.error("Fail to parse json string {} to {}", json, token.getType());
      return null;
    }
  }

  public static Map fromJson(String json) {
    return fromJson(json, Map.class);
  }

  public static void underscoreToCamel(SyncData data) {
    HashMap<String, Object> fields = data.getFields();
    HashMap<String, Object> tmp = new HashMap<>();
    for (Map.Entry<String, Object> e : fields.entrySet()) {
      String from = e.getKey();
      String to = underscoreToCamel(from);
      logger.debug("Rename field: {} -> {}", from, to);
      tmp.put(to, e.getValue());
    }
    fields.clear();
    fields.putAll(tmp);
  }

  public static String underscoreToCamel(String from) {
    char[] cs = from.toCharArray();
    StringBuilder sb = new StringBuilder(cs.length);
    boolean lastIsUnderscore = false;
    for (char c : cs) {
      if (c == '_') {
        lastIsUnderscore = true;
      } else if (Character.isAlphabetic(c)) {
        if (lastIsUnderscore && Character.isLowerCase(c)) {
          sb.append(Character.toUpperCase(c));
        } else {
          sb.append(c);
        }
        lastIsUnderscore = false;
      } else {
        logger.warn("Unsupported {} in {}", c, from);
      }
    }
    return sb.toString();
  }

  /**
   * @param key name for field which is byte[] in Java, which may come from blob type in db
   */
  public static void toStr(SyncData sync, String key) {
    Object value = sync.getField(key);
    if (value != null) {
      sync.updateField(key, new String((byte[]) value, java.nio.charset.StandardCharsets.UTF_8));
    }
  }

  public static void unsignedByte(SyncData sync, String key) {
    Object field = sync.getField(key);
    if (field != null) {
      sync.updateField(key, Byte.toUnsignedInt((byte)(int) field));
    }
  }

}
