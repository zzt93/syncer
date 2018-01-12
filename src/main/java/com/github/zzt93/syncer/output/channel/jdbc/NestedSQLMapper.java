package com.github.zzt93.syncer.output.channel.jdbc;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.expr.ParameterReplace;
import com.github.zzt93.syncer.config.pipeline.output.RowMapping;
import com.github.zzt93.syncer.output.mapper.KVMapper;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.StringJoiner;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.Assert;

/**
 * @author zzt
 */
public class NestedSQLMapper extends SQLMapper {

  /**
   * <a href="https://www.w3schools.com/sql/sql_insert_into_select.asp">insert into select</a>
   */
  private static final String INSERT_INTO_SELECT = "insert into `?0`.`?1` (?2) (?3)";
  private final Logger logger = LoggerFactory.getLogger(NestedSQLMapper.class);
  private final KVMapper kvMapper;
  private final RowMapping rowMapping;
  private final SpelExpressionParser parser;

  public NestedSQLMapper(RowMapping rowMapping, JdbcTemplate jdbcTemplate) {
    super(rowMapping, jdbcTemplate);
    this.rowMapping = rowMapping;
    parser = new SpelExpressionParser();
    kvMapper = new KVMapper(rowMapping.getRows(), new JdbcNestedQueryMapper());
  }

  private String[] join(HashMap<String, Object> map) {
    StringJoiner keys = new StringJoiner(",");
    ParameterizedString parameterizedString = null;
    HashMap<String, String> tmp = new HashMap<>();
    for (Entry<String, Object> entry : map.entrySet()) {
      keys.add(entry.getKey());
      Object value = entry.getValue();
      if (value instanceof ParameterizedString && value != parameterizedString) {
        parameterizedString = (ParameterizedString) value;
      } else {
        if (value instanceof String) {
          value  = "'" + StringEscapeUtils.escapeSql(value.toString()) + "''";
        }
        tmp.put(entry.getKey(), value.toString());
      }
    }
    Assert.notNull(parameterizedString, "[Impossible to be null]");
    parameterizedString.nameToAlias(tmp);
    return new String[]{keys.toString(), parameterizedString.getSql()};
  }

  @Override
  public String map(SyncData data) {
    if (!data.hasExtra()) {
      return super.map(data);
    }
    StandardEvaluationContext context = data.getContext();
    String schema = eval(rowMapping.getSchema(), context);
    String table = eval(rowMapping.getTable(), context);
    String id = eval(rowMapping.getId(), context);
    HashMap<String, Object> map = kvMapper.map(data);
    logger.debug("Convert SyncData to {}", map);
    switch (data.getType()) {
      case WRITE_ROWS:
        data.addRecord("id", id);
        String[] entry = join(map);
        return ParameterReplace
            .orderedParam(INSERT_INTO_SELECT, schema, table, entry[0], entry[1]);
    }
    throw new IllegalArgumentException("Unsupported row event type: " + data);
  }

  private String eval(String expr, StandardEvaluationContext context) {
    return parser
        .parseExpression(expr)
        .getValue(context, String.class);
  }

}
