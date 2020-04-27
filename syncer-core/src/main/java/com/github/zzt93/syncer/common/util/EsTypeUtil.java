package com.github.zzt93.syncer.common.util;

import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/**
 * @author zzt
 */
public class EsTypeUtil {

  /**
   * https://discuss.elastic.co/t/java-api-plainless-script-indexof-give-wrong-answer/139016/4
   */
  public static Object scriptConvert(Object value) {
    if (value instanceof Long) {
      return ((Long) value) <= Integer.MAX_VALUE ? ((Long) value).intValue() : value;
    } else if (value instanceof List) {
      for (ListIterator it = ((List) value).listIterator(); it.hasNext();){
        it.set(scriptConvert(it.next()));
      }
    } else if (value instanceof Map) {
      HashMap<Object, Object> res = new HashMap<>();
      for (Map.Entry<Object, Object> e : ((Map<Object, Object>) value).entrySet()) {
        res.put(scriptConvert(e.getKey()), scriptConvert(e.getValue()));
      }
      return res;
    }
    return value;
  }

}
