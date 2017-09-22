package com.github.zzt93.syncer.output;

import com.github.zzt93.syncer.common.SyncData;
import java.util.HashMap;
import java.util.Map;
import org.springframework.expression.ParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * @author zzt
 */
public class JsonMapper implements Mapper<SyncData, HashMap<String, Object>> {

  public static final String ROW_ALL = "row.*";
  public static final String ROW_FLATTEN = "row.*.flatten";
  public static final String EXTRA_ALL = "extra.*";
  public static final String EXTRA_FLATTEN = "extra.*.flatten";
  private final SpelExpressionParser parser = new SpelExpressionParser();
  private final Map<String, Object> mapping;

  public JsonMapper(Map<String, Object> mapping) {
    this.mapping = mapping;
  }

  public HashMap<String, Object> map(SyncData data) {
    HashMap<String, Object> res = new HashMap<>();
    mapToRes(data, mapping, res);
    return res;
  }

  private void mapToRes(SyncData src, Map<String, Object> mapping, HashMap<String, Object> res) {
    for (String key : mapping.keySet()) {
      Object o = mapping.get(key);
      if (o instanceof Map) {
        Map map = (Map) o;
        HashMap<String, Object> sub = new HashMap<>();
        mapToRes(src, map, sub);
        res.put(key, sub);
      } else if (o instanceof String) {
        String expr = (String) o;
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
            // TODO 9/20/17 check expr contains template
            String value = parser.parseExpression(expr, ParserContext.TEMPLATE_EXPRESSION)
                .getValue(new StandardEvaluationContext(src), String.class);
            res.put(key, value);
            break;
        }
      }
    }
  }
}
