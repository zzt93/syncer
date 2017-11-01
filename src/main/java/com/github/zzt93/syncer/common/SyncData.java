package com.github.zzt93.syncer.common;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import java.util.Arrays;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

/**
 * @author zzt
 */
public class SyncData {

  private final Logger logger = LoggerFactory.getLogger(SyncData.class);

  private EventType type;
  private String action;
  /*
   * The following is data field
   */
  private final HashMap<String, Object> records = new HashMap<>();
  private final HashMap<String, Object> extra = new HashMap<>();
  private final StandardEvaluationContext context;
  private String schema;
  private String table;
  /**
   * table primary key
   */
  private String id;

  private SyncByQueryES syncByQuery = new SyncByQueryES();


  public SyncData(TableMapEventData tableMap, String primaryKey,
      HashMap<String, Object> row, EventType type) {
    this.type = type;
    action = type.toString();
    Object key = row.get(primaryKey);
    if (key != null) {
      id = key.toString();
    }
    schema = tableMap.getDatabase();
    table = tableMap.getTable();
    records.putAll(row);
    context = new StandardEvaluationContext(this);
  }

  public SyncData() {
    context = new StandardEvaluationContext(this);
  }

  public SyncData setType(EventType type) {
    this.type = type;
    return this;
  }

  public SyncData setAction(String action) {
    this.action = action;
    return this;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getTable() {
    return table;
  }

  public String getAction() {
    return action;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public EventType getType() {
    return type;
  }

  public void addExtra(String key, Object value) {
    extra.put(key, value);
  }

  public void addRecord(String key, Object value) {
    if (value == null) {
      logger.warn("Adding column({}) with null, discarded", key);
    }
    records.put(key, value);
  }

  public void renameRecord(String oldKey, String newKey) {
    if (records.containsKey(oldKey)) {
      records.put(newKey, records.get(oldKey));
      records.remove(oldKey);
    } else {
      logger.warn("No such record name (maybe filtered out) (): {} in {}.{}", oldKey, schema, table);
    }
  }

  public void removeRecord(String key) {
    records.remove(key);
  }

  public void removeRecords(String... keys) {
    for (String colName : keys) {
      records.remove(colName);
    }
  }

  public boolean containRecord(String key) {
    return records.containsKey(key);
  }

  public void updateRecord(String key, Object value) {
    if (records.containsKey(key)) {
      records.put(key, value);
    } else {
      logger.warn("No such record name (maybe filtered out): {} in {}.{}", key, schema, table);
    }
  }

  public StandardEvaluationContext getContext() {
    return context;
  }

  public HashMap<String, Object> getRecords() {
    return records;
  }

  public HashMap<String, Object> getExtra() {
    return extra;
  }

  public Object getRecordValue(String key) {
    Assert.isTrue(records.containsKey(key), records.toString() + "[No such record]: " + key);
    return records.get(key);
  }

  public HashMap<String, Object> getSyncBy() {
    return syncByQuery.getSyncBy();
  }

  public boolean isSyncWithoutId() {
    return syncByQuery.isSyncWithoutId();
  }

  /**
   * update/delete by query
   */
  public SyncByQueryES syncByQuery() {
    return syncByQuery;
  }

  public ExtraQueryES extraQuery(String indexName, String typeName) {
    return new ExtraQueryES(this).setIndexName(indexName).setTypeName(typeName);
  }

  /**
   * ----------- update/delete by query -----------
   */
  static class SyncByQueryES {

    private static final Logger logger = LoggerFactory.getLogger(SyncByQueryES.class);

    private final HashMap<String, Object> syncBy = new HashMap<>();

    public SyncByQueryES filter(String syncWithCol, Object value) {
      syncBy.put(syncWithCol, value);
      return this;
    }

    boolean isSyncWithoutId() {
      return !syncBy.isEmpty();
    }

    HashMap<String, Object> getSyncBy() {
      return syncBy;
    }

  }

  /**
   * ----------- add separated query before index ------------
   */
  public static class ExtraQueryES {

    private static final Logger logger = LoggerFactory.getLogger(ExtraQueryES.class);
    private final HashMap<String, Object> queryBy = new HashMap<>();
    private final SyncData data;
    private String queryId;
    private String indexName;
    private String typeName;
    private String[] target;
    private String[] cols;

    private ExtraQueryES(SyncData data) {
      this.data = data;
    }

    public String getTypeName() {
      return typeName;
    }

    ExtraQueryES setTypeName(String typeName) {
      this.typeName = typeName;
      return this;
    }

    public ExtraQueryES filter(String field, Object value) {
      queryBy.put(field, value);
      return this;
    }

    public ExtraQueryES select(String... field) {
      target = field;
      return this;
    }

    public ExtraQueryES addRecord(String... cols) {
      if (cols.length != target.length) {
        throw new InvalidConfigException("Column length is not same as query select result");
      }
      this.cols = cols;
      for (String col : cols) {
        data.getRecords().put(col, this);
      }
      return this;
    }

    public String getIndexName() {
      return indexName;
    }

    ExtraQueryES setIndexName(String indexName) {
      this.indexName = indexName;
      return this;
    }

    public HashMap<String, Object> getQueryBy() {
      return queryBy;
    }

    public String[] getTarget() {
      return target;
    }

    public String getCol(int i) {
      return cols[i];
    }

    @Override
    public String toString() {
      return "ExtraQueryES{select " + Arrays.toString(target) + " as " + Arrays.toString(cols)
          + " from " + indexName + "." + typeName + " where " + queryBy +"}";
    }
  }

}
