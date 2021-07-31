package com.github.zzt93.syncer.consumer.output.channel.jdbc;

import com.github.zzt93.syncer.common.data.Mapper;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.exception.InvalidSyncDataException;
import com.github.zzt93.syncer.common.expr.ParameterReplace;
import com.github.zzt93.syncer.common.util.SQLHelper;
import com.github.zzt93.syncer.data.SimpleEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.StringJoiner;

import static com.github.zzt93.syncer.data.SimpleEventType.UPDATE;
import static com.github.zzt93.syncer.data.SimpleEventType.WRITE;

/**
 * @author zzt
 */
public class SQLMapper implements Mapper<SyncData, String> {

  // TODO 2019-11-23 ignore dup is right?
  static final String INSERT_INTO_VALUES = "insert into `?0`.`?1` (?2) values (?3)";
  static final String DELETE_FROM_WHERE_ID = "delete from `?0`.`?1` where id = ?2";
  static final String UPDATE_SET_WHERE_ID = "update `?0`.`?1` set ?3 where id = ?2";
  static final String UPDATE_SET_WHERE = "update `?0`.`?1` set ?3 where ?2";
  private final Logger logger = LoggerFactory.getLogger(SQLMapper.class);

  SQLMapper() {
  }

  @Override
  public String map(SyncData data) {
    if (data.hasExtraQuery()) {
      logger.error("MySQLChannel doesn't support extraQuery, ignored: {}", data);
    }
    String schema = data.getDb();
    String table = data.getTable();
    String id = data.getDbId();
    HashMap<String, Object> map = data.getFields();
    switch (data.getType()) {
      case WRITE:
        String[] entry = join(map, WRITE);
        return ParameterReplace
            .orderedParam(INSERT_INTO_VALUES, schema, table, entry[0], entry[1]);
      case DELETE:
        return ParameterReplace.orderedParam(DELETE_FROM_WHERE_ID, schema, table, id);
      case UPDATE:
        if (id != null) {
          return ParameterReplace.orderedParam(UPDATE_SET_WHERE_ID, schema, table, id, join(map, UPDATE)[0]);
        } else {
          HashMap<String, Object> syncBy = data.getSyncBy();
          if (syncBy == null) {
            throw new InvalidSyncDataException("Ignore invalid SyncData: update row without [id] and/or [syncByQuery].", data);
          }
          String filterCondition = join(syncBy, UPDATE)[0];
          return ParameterReplace.orderedParam(UPDATE_SET_WHERE, schema, table, filterCondition, join(map,
              data.getType())[0]);
        }
      default:
        throw new IllegalArgumentException("Unsupported row event type: " + data);
    }
  }

  private String[] join(final HashMap<String, Object> map, SimpleEventType type) {
    switch (type) {
      case WRITE:
        StringJoiner keys = new StringJoiner(",");
        StringJoiner values = new StringJoiner(",");
        for (Entry<String, Object> entry : map.entrySet()) {
          keys.add(SQLHelper.wrapCol(entry.getKey()));
          values.add(SQLHelper.inSQL(entry.getValue()));
        }
        return new String[]{keys.toString(), values.toString()};
      case UPDATE:
        StringJoiner kv = new StringJoiner(",");
        for (Entry<String, Object> entry : map.entrySet()) {
          String condition = "" + SQLHelper.wrapCol(entry.getKey()) + "=" + SQLHelper.inSQL(entry.getValue());
          kv.add(condition);
        }
        return new String[]{kv.toString()};
      default:
        throw new IllegalArgumentException("Unsupported row event type: " + type);
    }
  }

}
