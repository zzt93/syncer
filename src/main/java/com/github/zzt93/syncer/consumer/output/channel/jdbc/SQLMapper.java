package com.github.zzt93.syncer.consumer.output.channel.jdbc;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.expr.ParameterReplace;
import com.github.zzt93.syncer.config.pipeline.output.mysql.RowMapping;
import com.github.zzt93.syncer.consumer.output.mapper.KVMapper;
import com.github.zzt93.syncer.consumer.output.mapper.Mapper;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.StringJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * @author zzt
 */
public class SQLMapper implements Mapper<SyncData, String> {

  private static final String INSERT_INTO_VALUES = "insert into `?0`.`?1` (?2) values (?3)";
  private static final String DELETE_FROM_WHERE_ID = "delete from `?0`.`?1` where id = ?2";
  private static final String UPDATE_SET_WHERE_ID = "update `?0`.`?1` set ?3 where id = ?2";
  private final Logger logger = LoggerFactory.getLogger(SQLMapper.class);
  private final KVMapper kvMapper;
  private final Expression schema;
  private final Expression table;
  private final Expression id;

  public SQLMapper(RowMapping rowMapping, JdbcTemplate jdbcTemplate) {
    SpelExpressionParser parser = new SpelExpressionParser();

    schema = parser.parseExpression(rowMapping.getSchema());
    table = parser.parseExpression(rowMapping.getTable());
    id = parser.parseExpression(rowMapping.getId());

    kvMapper = new KVMapper(rowMapping.getRows(), new JdbcNestedQueryMapper());
  }

  @Override
  public String map(SyncData data) {
    StandardEvaluationContext context = data.getContext();
    String schema = eval(this.schema, context);
    String table = eval(this.table, context);
    String id = eval(this.id, context);
    HashMap<String, Object> map = kvMapper.map(data);
    logger.debug("Convert SyncData to {}", map);
    // TODO 18/1/24 replace with `PreparedStatement`?
    switch (data.getType()) {
      case WRITE_ROWS:
        String[] entry = join(map, data.getType());
        return ParameterReplace
            .orderedParam(INSERT_INTO_VALUES, schema, table, entry[0], entry[1]);
      case DELETE_ROWS:
        return ParameterReplace.orderedParam(DELETE_FROM_WHERE_ID, schema, table, id);
      case UPDATE_ROWS:
        return ParameterReplace.orderedParam(UPDATE_SET_WHERE_ID, schema, table, id, join(map,
            data.getType())[0]);
    }
    throw new IllegalArgumentException("Unsupported row event type: " + data);
  }

  private String[] join(HashMap<String, Object> map, EventType type) {
    switch (type) {
      case WRITE_ROWS:
        StringJoiner keys = new StringJoiner(",");
        StringJoiner values = new StringJoiner(",");
        for (Entry<String, Object> entry : map.entrySet()) {
          keys.add(entry.getKey());
          values.add(SQLHelper.inSQL(entry.getValue()));
        }
        return new String[]{keys.toString(), values.toString()};
      case UPDATE_ROWS:
        StringJoiner kv = new StringJoiner(",");
        for (Entry<String, Object> entry : map.entrySet()) {
          String condition = "" + entry.getKey() + "=" + SQLHelper.inSQL(entry.getValue());
          kv.add(condition);
        }
        return new String[]{kv.toString()};
    }
    throw new IllegalArgumentException("Unsupported row event type: " + type);
  }

  private String eval(Expression expr, StandardEvaluationContext context) {
    return expr.getValue(context, String.class);
  }

}
