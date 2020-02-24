package com.github.zzt93.syncer.consumer.output.channel.mapper;

import com.github.zzt93.syncer.common.data.ExtraQuery;
import com.github.zzt93.syncer.common.data.Mapper;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author zzt
 */
public class KVMapper implements Mapper<SyncData, HashMap<String, Object>> {

  public static final String ROW_ALL = "fields.*";
  public static final String ROW_FLATTEN = "fields.*.flatten";
  public static final String EXTRA_ALL = "extra.*";
  public static final String EXTRA_FLATTEN = "extra.*.flatten";
  public static final String FAKE_KEY = "any.Key";
  private final Logger logger = LoggerFactory.getLogger(KVMapper.class);
  private final Map<String, Object> mapping;

  public KVMapper(Map<String, Object> mapping) {
    this.mapping = new LinkedHashMap<>();
    initMapping(mapping, this.mapping, new SpelExpressionParser());
  }

  public KVMapper(HashMap<String, Object> mapping) {
    this.mapping = new HashMap<>();
    initMapping(mapping, this.mapping, new SpelExpressionParser());
  }

  public HashMap<String, Object> map(SyncData data) {
    HashMap<String, Object> res = new HashMap<>();
    mapToRes(data, mapping, res, true);
    logger.debug("SyncData json: {}", res);
    return res;
  }

  private void initMapping(Map<String, Object> mapping, Map<String, Object> res,
      SpelExpressionParser parser) {
    for (Entry<String, Object> entry : mapping.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      if (value instanceof Map) {
        Map map = (Map) value;
        initMapping(map, res, parser);
      } else if (value instanceof String) {
        String expr = (String) value;
        switch (expr) {
          case ROW_ALL:
          case EXTRA_ALL:
          case ROW_FLATTEN:
          case EXTRA_FLATTEN:
            res.put(key, expr);
            break;
          default:
            Expression expression = parser.parseExpression(expr);
            res.put(key, expression);
            break;
        }
      } else {
        res.put(key, value);
      }
    }
  }

  private static void mapToRes(SyncData src, Map<String, Object> mapping, HashMap<String, Object> res,
      boolean interpretSpecialString) {
    for (String key : mapping.keySet()) {
      Object value = mapping.get(key);
      if (value instanceof Expression) {
        res.put(key, ((Expression) value).getValue(src.getContext()));
      } else if (value instanceof Map) {
        Map map = (Map) value;
        mapObj(src, res, key, map, interpretSpecialString);
      } else if (value instanceof String && interpretSpecialString) {
        String expr = (String) value;
        switch (expr) {
          case ROW_ALL:
            mapObj(src, res, key, src.getFields(), false);
            break;
          case EXTRA_ALL:
            res.put(key, src.getExtras());
            break;
          case ROW_FLATTEN:
            mapToRes(src, src.getFields(), res, false);
            break;
          case EXTRA_FLATTEN:
            res.putAll(src.getExtras());
            break;
          default:
            throw new InvalidConfigException("Unknown special expression: " + expr);
        }
      } else if (value instanceof ExtraQuery) {
        res.put(key,  ((ExtraQuery) value).getQueryResult(key));
      } else {
        res.put(key, value);
      }
    }
  }

  public static void map(HashMap<String, Object> src, HashMap<String, Object> res) {
    mapToRes(null, src, res, false);
  }

  private static void mapObj(SyncData src, HashMap<String, Object> res, String objKey, Map objMap,
      boolean interpretSpecialString) {
    HashMap<String, Object> sub = new HashMap<>();
    mapToRes(src, objMap, sub, interpretSpecialString);
    res.put(objKey, sub);
  }
}
