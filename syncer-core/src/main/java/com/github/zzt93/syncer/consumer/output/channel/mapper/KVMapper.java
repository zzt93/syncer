package com.github.zzt93.syncer.consumer.output.channel.mapper;

import com.github.zzt93.syncer.common.data.Mapper;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.util.SyncDataTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author zzt
 */
public class KVMapper implements Mapper<SyncData, HashMap<String, Object>> {

  public static final String FAKE_KEY = "any.Key";
  private final Logger logger = LoggerFactory.getLogger(KVMapper.class);
  private final Map<String, Object> mapping;

  public KVMapper(Map<String, Object> mapping) {
    HashMap<String, Object> tmp = new HashMap<>();
    initMapping(mapping, tmp, new SpelExpressionParser());
    this.mapping = Collections.unmodifiableMap(tmp);
  }

  public KVMapper(HashMap<String, Object> mapping) {
    HashMap<String, Object> tmp = new HashMap<>();
    initMapping(mapping, tmp, new SpelExpressionParser());
    this.mapping = Collections.unmodifiableMap(tmp);
  }

  public HashMap<String, Object> map(SyncData data) {
    HashMap<String, Object> res = new HashMap<>();
    SyncDataTypeUtil.mapToJson(data, mapping, res, true);
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
          case SyncDataTypeUtil.ROW_ALL:
          case SyncDataTypeUtil.EXTRA_ALL:
          case SyncDataTypeUtil.ROW_FLATTEN:
          case SyncDataTypeUtil.EXTRA_FLATTEN:
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

}
