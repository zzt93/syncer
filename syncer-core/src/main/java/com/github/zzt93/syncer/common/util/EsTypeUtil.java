package com.github.zzt93.syncer.common.util;

/**
 * @author zzt
 */
public class EsTypeUtil {

  public static Object convertType(Object value) {
    if (value instanceof Long) {
      return ((Long) value) < Integer.MAX_VALUE ? ((Long) value).intValue() : value;
    }
    return value;
  }

}
