package com.github.zzt93.syncer.output.mapper;

import com.github.zzt93.syncer.common.ExtraQuery;
import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.output.channel.ExtraQueryMapper;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * @author zzt
 */
public class KVMapper implements Mapper<SyncData, HashMap<String, Object>> {

  public static final String ROW_ALL = "records.*";
  public static final String ROW_FLATTEN = "records.*.flatten";
  public static final String EXTRA_ALL = "extra.*";
  public static final String EXTRA_FLATTEN = "extra.*.flatten";
  public static final String FAKE_KEY = "any.Key";
  private final Logger logger = LoggerFactory.getLogger(KVMapper.class);
  private final SpelExpressionParser parser = new SpelExpressionParser();
  private final Map<String, Object> mapping;
  private final ExtraQueryMapper queryMapper;

  public KVMapper(Map<String, Object> mapping) {
    this.mapping = mapping;
    queryMapper = null;
  }

  public KVMapper(HashMap<String, Object> mapping, ExtraQueryMapper extraQueryMapper) {
    this.mapping = mapping;
    this.queryMapper = extraQueryMapper;
  }

  public HashMap<String, Object> map(SyncData data) {
    HashMap<String, Object> res = new HashMap<>();
    mapToRes(data, mapping, res, true);
    logger.info("SyncData json: " + res);
    return res;
  }

  private void mapToRes(SyncData src, Map<String, Object> mapping, HashMap<String, Object> res,
      boolean parseString) {
    Map<String, Object> queryResult = new HashMap<>();
    for (String key : mapping.keySet()) {
      Object value = mapping.get(key);
      if (value instanceof Map) {
        Map map = (Map) value;
        mapObj(src, res, key, map, true);
      } else if (value instanceof String) {
        String expr = (String) value;
        switch (expr) {
          case ROW_ALL:
            handleAll(src, res, key, src.getRecords());
            break;
          case EXTRA_ALL:
            handleAll(src, res, key, src.getExtra());
            break;
          case ROW_FLATTEN:
            mapToRes(src, src.getRecords(), res, false);
            break;
          case EXTRA_FLATTEN:
            mapToRes(src, src.getExtra(), res, false);
            break;
          default:
            if (parseString) {
              String parsedValue = parser.parseExpression(expr)
                  .getValue(src.getContext(), String.class);
              res.put(key, parsedValue);
            } else {
              res.put(key, value);
            }
            break;
        }
      } else if (value instanceof ExtraQuery) {
        if (queryMapper != null) {
          if (!queryResult.containsKey(key)) {
            queryResult.putAll(queryMapper.map((ExtraQuery) value));
          }
          if(!queryResult.containsKey(key)) {
            logger.warn("Fail to query record {} by {}", key, value);
          }
          res.put(key, queryResult.get(key));
        } else {
          logger.warn("Not config `enable-extra-query` in `request-mapping`, `extraQuery()` is ignored");
        }
      } else {
        res.put(key, value);
      }
    }
  }

  private void handleAll(SyncData src, HashMap<String, Object> res, String key,
      HashMap<String, Object> nested) {
    HashMap<String, Object> map = new HashMap<>();
    map.put(key, nested);
    mapObj(src, res, key, map, false);
  }

  private void mapObj(SyncData src, HashMap<String, Object> res, String objKey, Map objMap,
      boolean parseString) {
    HashMap<String, Object> sub = new HashMap<>();
    mapToRes(src, objMap, sub, parseString);
    res.put(objKey, sub);
  }
}
