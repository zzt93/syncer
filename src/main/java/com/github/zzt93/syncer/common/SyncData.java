package com.github.zzt93.syncer.common;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.zzt93.syncer.common.event.RowEvent;
import java.lang.reflect.Field;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ReflectionUtils;

/**
 * @author zzt
 */
public class SyncData {

  private final EventType type;
  private final String schema;
  private final String table;
  private final HashMap<String, Object> row = new HashMap<>();
  private final HashMap<String, Object> extra = new HashMap<>();
  private final Logger logger = LoggerFactory.getLogger(SyncData.class);

  public SyncData(EventType type, String schema, String table) {
    this.type = type;
    this.schema = schema;
    this.table = table;
  }

  public SyncData(RowEvent rowEvent, EventType type) {
    this.type = type;
    TableMapEventData tableMap = rowEvent.getTableMap();
    schema = tableMap.getDatabase();
    table = tableMap.getTable();
    row.putAll(rowEvent.getData());
  }

  public String getTable() {
    return table;
  }

  public SyncData addExtra(String colName, Object value) {
    extra.put(colName, value);
    return this;
  }

  public Object getData(String key) {
    // check field
    for (Field field : SyncData.class.getDeclaredFields()) {
      if (field.getName().equals(key)) {
        return ReflectionUtils.getField(field, this);
      }
    }

    if (row.containsKey(key) && extra.containsKey(key)) {
      logger.warn("Duplicate key '{}' in row data and extra, behaviour is undefined", key);
    }
    return row.containsKey(key) ? row.get(key) : extra.get(key);
  }

  public String getSchema() {
    return schema;
  }

  public EventType getType() {
    return type;
  }
}
