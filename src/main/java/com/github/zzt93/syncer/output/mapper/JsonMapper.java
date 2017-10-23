package com.github.zzt93.syncer.output.mapper;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.SyncData.ExtraQueryES;
import com.github.zzt93.syncer.output.channel.elastic.ESQueryMapper;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * @author zzt
 */
public class JsonMapper implements Mapper<SyncData, HashMap<String, Object>> {

  public static final String ROW_ALL = "row.*";
  public static final String ROW_FLATTEN = "row.*.flatten";
  public static final String EXTRA_ALL = "extra.*";
  public static final String EXTRA_FLATTEN = "extra.*.flatten";
  public static final String FAKE_KEY = "any.Key";
  private final Logger logger = LoggerFactory.getLogger(JsonMapper.class);
  private final SpelExpressionParser parser = new SpelExpressionParser();
  private final Map<String, Object> mapping;
  private final ESQueryMapper queryMapper;

  public JsonMapper(Map<String, Object> mapping) {
    this.mapping = mapping;
    queryMapper = null;
  }

  public JsonMapper(HashMap<String, Object> mapping, ESQueryMapper esQueryMapper) {
    this.mapping = mapping;
    this.queryMapper = esQueryMapper;
  }

  public HashMap<String, Object> map(SyncData data) {
    HashMap<String, Object> res = new HashMap<>();
    mapToRes(data, mapping, res);
    return res;
  }

  private void mapToRes(SyncData src, Map<String, Object> mapping, HashMap<String, Object> res) {
    Map<String, Object> queryResult = null;
    for (String key : mapping.keySet()) {
      Object value = mapping.get(key);
      if (value instanceof Map) {
        Map map = (Map) value;
        HashMap<String, Object> sub = new HashMap<>();
        mapToRes(src, map, sub);
        res.put(key, sub);
      } else if (value instanceof String) {
        String expr = (String) value;
        switch (expr) {
          case ROW_ALL:
            res.put(key, src.getRow());
            break;
          case EXTRA_ALL:
            res.put(key, src.getExtra());
            break;
          case ROW_FLATTEN:
            res.putAll(src.getRow());
            break;
          case EXTRA_FLATTEN:
            res.putAll(src.getExtra());
            break;
          default:
            String parsedValue = parser.parseExpression(expr)
                .getValue(src.getContext(), String.class);
            res.put(key, parsedValue);
            break;
        }
      } else if (value instanceof ExtraQueryES) {
        if (queryMapper != null) {
          if (queryResult == null) {
            queryResult = queryMapper.map((ExtraQueryES) value);
          }
          assert queryResult.containsKey(key);
          res.put(key, queryResult.get(key));
        } else {
          logger.warn("Not config `query-mapping` in `request-mapping`, `extraQuery()` is ignored");
        }
      }
    }
  }
}
