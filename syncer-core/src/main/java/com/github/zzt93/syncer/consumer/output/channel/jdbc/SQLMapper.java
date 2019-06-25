package com.github.zzt93.syncer.consumer.output.channel.jdbc;

import com.github.zzt93.syncer.common.data.Mapper;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.exception.InvalidSyncDataException;
import com.github.zzt93.syncer.common.expr.ParameterReplace;
import com.github.zzt93.syncer.config.consumer.output.mysql.RowMapping;
import com.github.zzt93.syncer.consumer.output.channel.mapper.KVMapper;
import com.github.zzt93.syncer.data.SimpleEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.StringJoiner;

/**
 * @author zzt
 */
public class SQLMapper implements Mapper<SyncData, String> {

  private static final String INSERT_INTO_VALUES = "insert into `?0`.`?1` (?2) values (?3)";
  private static final String UPSERT = "insert into `?0`.`?1` (?2) values (?3) ON DUPLICATE KEY UPDATE ?4" ;
  private static final String DELETE_FROM_WHERE_ID = "delete from `?0`.`?1` where id = ?2";
  private static final String UPDATE_SET_WHERE_ID = "update `?0`.`?1` set ?3 where id = ?2";
  private static final String UPDATE_SET_WHERE = "update `?0`.`?1` set ?3 where ?2";
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
    String schema = evalString(this.schema, context);
    String table = evalString(this.table, context);
    String id = evalString(this.id, context);
    HashMap<String, Object> map = kvMapper.map(data);
    logger.debug("Convert SyncData to {}", map);
    switch (data.getType()) {
      case WRITE: {
        String[] entry = join(map, data.getType());
        return ParameterReplace
            .orderedParam(INSERT_INTO_VALUES, schema, table, entry[0], entry[1]);
      }
      case DELETE:
        return ParameterReplace.orderedParam(DELETE_FROM_WHERE_ID, schema, table, id);
      case UPDATE:
        if (id != null) {
          return ParameterReplace.orderedParam(UPDATE_SET_WHERE_ID, schema, table, id, join(map,
              data.getType())[0]);
        } else {
          HashMap<String, Object> syncBy = data.getSyncBy();
          if (syncBy == null) {
            throw new InvalidSyncDataException("Ignore invalid SyncData: update row without [id] and/or [syncByQuery].", data);
          }
          String filterCondition = join(syncBy, SimpleEventType.UPDATE)[0];
          return ParameterReplace.orderedParam(UPDATE_SET_WHERE, schema, table, filterCondition, join(map,
              data.getType())[0]);
        }
      case UPSERT: {
        String[] entry = join(map, data.getType());
        return ParameterReplace
            .orderedParam(UPSERT, schema, table, entry[0], entry[1], entry[2]);
      }
      default:
        throw new IllegalArgumentException("Unsupported row event type: " + data);
    }
  }

  private String[] join(HashMap<String, Object> map, SimpleEventType type) {
    switch (type) {
      case UPSERT: {
        InsertKV insertKV = new InsertKV(map).invoke();
        StringJoiner keys = insertKV.getKeys();
        StringJoiner values = insertKV.getValues();
        return new String[]{keys.toString(), values.toString(), fieldSetSql(map).toString()};
      }
      case WRITE:
        InsertKV insertKV = new InsertKV(map).invoke();
        StringJoiner keys = insertKV.getKeys();
        StringJoiner values = insertKV.getValues();
        return new String[]{keys.toString(), values.toString()};
      case UPDATE:
        StringJoiner kv = fieldSetSql(map);
        return new String[]{kv.toString()};

      default:
        throw new IllegalArgumentException("Unsupported row event type: " + type);
    }
  }

  private StringJoiner fieldSetSql(HashMap<String, Object> map) {
    StringJoiner kv = new StringJoiner(",");
    for (Entry<String, Object> entry : map.entrySet()) {
      String condition = "" + SQLHelper.wrapCol(entry.getKey()) + "=" + SQLHelper.inSQL(entry.getValue());
      kv.add(condition);
    }
    return kv;
  }

  String evalString(Expression expr, StandardEvaluationContext context) {
    return expr.getValue(context, String.class);
  }

  private class InsertKV {
    private HashMap<String, Object> map;
    private StringJoiner keys;
    private StringJoiner values;

    InsertKV(HashMap<String, Object> map) {
      this.map = map;
    }

    StringJoiner getKeys() {
      return keys;
    }

    StringJoiner getValues() {
      return values;
    }

    InsertKV invoke() {
      keys = new StringJoiner(",");
      values = new StringJoiner(",");
      for (Entry<String, Object> entry : map.entrySet()) {
        keys.add(SQLHelper.wrapCol(entry.getKey()));
        values.add(SQLHelper.inSQL(entry.getValue()));
      }
      return this;
    }
  }
}
