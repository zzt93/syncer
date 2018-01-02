package com.github.zzt93.syncer.output.channel.jdbc;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.expr.ParameterReplace;
import com.github.zzt93.syncer.config.pipeline.output.RowMapping;
import com.github.zzt93.syncer.output.mapper.KVMapper;
import com.github.zzt93.syncer.output.mapper.Mapper;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.StringJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * @author zzt
 */
public class JdbcMapper implements Mapper<SyncData, String> {

  private static final String INSERT_INTO_VALUES = "insert into `?0`.`?1` (?2) values (?3)";
  private static final String DELETE_FROM_WHERE_ID = "delete from `?0`.`?1` where id = `?2`";
  private static final String UPDATE_SET_WHERE_ID = "update `?0`.`?1` set ?3 where id = `?2`";
  private final Logger logger = LoggerFactory.getLogger(JdbcMapper.class);
  private final KVMapper kvMapper;
  private final RowMapping rowMapping;
  private final SpelExpressionParser parser;

  public JdbcMapper(RowMapping rowMapping) {
    this.rowMapping = rowMapping;
    parser = new SpelExpressionParser();
    kvMapper = new KVMapper(rowMapping.getRows());
  }

  @Override
  public String map(SyncData data) {
    StandardEvaluationContext context = data.getContext();
    String schema = eval(rowMapping.getSchema(), context);
    String table = eval(rowMapping.getTable(), context);
    String id = eval(rowMapping.getId(), context);
    HashMap<String, Object> map = kvMapper.map(data);
    switch (data.getType()) {
      case WRITE_ROWS:
        data.addRecord("id", id);
        String[] strings = join(map);
        return ParameterReplace
            .orderedParam(INSERT_INTO_VALUES, schema, table, strings[0], strings[1]);
      case DELETE_ROWS:
        return ParameterReplace.orderedParam(DELETE_FROM_WHERE_ID, schema, table, id);
      case UPDATE_ROWS:
        data.removeRecord("id");
        String str = map.toString();
        return ParameterReplace.orderedParam(UPDATE_SET_WHERE_ID, schema, table, id,
            str.substring(1, str.length() - 1));
    }
    throw new IllegalArgumentException("Unsupported row event type: " + data);
  }

  private static String[] join(HashMap<String, Object> map) {
    StringJoiner keys = new StringJoiner(",");
    StringJoiner values = new StringJoiner(",");
    for (Entry<String, Object> entry : map.entrySet()) {
      keys.add(entry.getKey());
      values.add(entry.getValue().toString());
    }
    return new String[]{keys.toString(), values.toString()};
  }

  private String eval(String expr, StandardEvaluationContext context) {
    return parser
        .parseExpression(expr)
        .getValue(context, String.class);
  }

}
