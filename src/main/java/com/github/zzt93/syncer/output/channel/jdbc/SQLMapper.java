package com.github.zzt93.syncer.output.channel.jdbc;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.expr.ParameterReplace;
import com.github.zzt93.syncer.config.pipeline.output.RowMapping;
import com.github.zzt93.syncer.output.mapper.KVMapper;
import com.github.zzt93.syncer.output.mapper.Mapper;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.StringJoiner;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
  private final RowMapping rowMapping;
  private final SpelExpressionParser parser;

  public SQLMapper(RowMapping rowMapping, JdbcTemplate jdbcTemplate) {
    this.rowMapping = rowMapping;
    parser = new SpelExpressionParser();
    kvMapper = new KVMapper(rowMapping.getRows(), new JdbcExtraQueryMapper(jdbcTemplate));
  }

  @Override
  public String map(SyncData data) {
    StandardEvaluationContext context = data.getContext();
    String schema = eval(rowMapping.getSchema(), context);
    String table = eval(rowMapping.getTable(), context);
    String id = eval(rowMapping.getId(), context);
    HashMap<String, Object> map = kvMapper.map(data);
    logger.debug("Convert SyncData to {}", map);
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
          Object value = entry.getValue();
          if (value instanceof String) {
            value = "'" + StringEscapeUtils.escapeSql(value.toString()) + "'";
          }
          values.add(value.toString());
        }
        return new String[]{keys.toString(), values.toString()};
      case UPDATE_ROWS:
        StringJoiner kv = new StringJoiner(",");
        for (Entry<String, Object> entry : map.entrySet()) {
          String tmp = "" + entry.getKey() + "='" +
              StringEscapeUtils.escapeSql(entry.getValue().toString()) + "'";
          kv.add(tmp);
        }
        return new String[]{kv.toString()};
    }
    throw new IllegalArgumentException("Unsupported row event type: " + type);
  }

  private String eval(String expr, StandardEvaluationContext context) {
    return parser
        .parseExpression(expr)
        .getValue(context, String.class);
  }

}
