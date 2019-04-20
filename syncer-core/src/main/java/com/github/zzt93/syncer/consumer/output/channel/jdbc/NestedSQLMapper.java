package com.github.zzt93.syncer.consumer.output.channel.jdbc;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.expr.ParameterReplace;
import com.github.zzt93.syncer.config.consumer.output.mysql.RowMapping;
import com.github.zzt93.syncer.consumer.output.channel.mapper.KVMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.StringJoiner;

/**
 * @author zzt
 */
public class NestedSQLMapper extends SQLMapper {

  /**
   * <a href="https://www.w3schools.com/sql/sql_insert_into_select.asp">insert into select</a> the
   * `?3` is a select clause
   */
  private static final String INSERT_INTO_SELECT = "insert into `?0`.`?1` (?2) (?3)";
  private final Logger logger = LoggerFactory.getLogger(NestedSQLMapper.class);
  private final KVMapper kvMapper;
  private final Expression schema;
  private final Expression table;

  public NestedSQLMapper(RowMapping rowMapping, JdbcTemplate jdbcTemplate) {
    super(rowMapping, jdbcTemplate);
    SpelExpressionParser parser = new SpelExpressionParser();
    schema = parser.parseExpression(rowMapping.getSchema());
    table = parser.parseExpression(rowMapping.getTable());
    kvMapper = new KVMapper(rowMapping.getRows(), new JdbcNestedQueryMapper());
  }

  private String[] join(HashMap<String, Object> map) {
    StringJoiner keys = new StringJoiner(",");
    ParameterizedString parameterizedString = null;
    HashMap<String, String> tmp = new HashMap<>();
    for (Entry<String, Object> entry : map.entrySet()) {
      keys.add(SQLHelper.wrapCol(entry.getKey()));
      Object value = entry.getValue();
      if (value instanceof ParameterizedString && value != parameterizedString) {
        // TODO 18/3/8 multiple query, change to select .. a join b
        // JdbcNestedQueryMapper & ParameterizedString
        parameterizedString = (ParameterizedString) value;
      } else {
        tmp.put(entry.getKey(), SQLHelper.inSQL(value));
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
    String schema = evalString(this.schema, context);
    String table = evalString(this.table, context);
    HashMap<String, Object> map = kvMapper.map(data);
    logger.debug("Convert SyncData to {}", map);
    switch (data.getType()) {
      case WRITE:
        String[] entry = join(map);
        return ParameterReplace
            .orderedParam(INSERT_INTO_SELECT, schema, table, entry[0], entry[1]);
      default:
        throw new IllegalArgumentException("Unsupported row event type: " + data);
    }
  }

}
