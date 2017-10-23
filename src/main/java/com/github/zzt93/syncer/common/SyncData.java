package com.github.zzt93.syncer.common;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
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

  private final EventType type;
  private final HashMap<String, Object> row = new HashMap<>();
  private final HashMap<String, Object> extra = new HashMap<>();
  private final StandardEvaluationContext context;
  private String schema;
  private String table;
  /**
   * table primary key
   */
  private String id;

  private SyncByQueryES syncByQuery = new SyncByQueryES();
  private SyncByQueryES syncByQueryES = new SyncByQueryES();


  public SyncData(TableMapEventData tableMap, String primaryKey,
      HashMap<String, Object> row, EventType type) {
    this.type = type;
    Object key = row.get(primaryKey);
    if (key != null) {
      id = key.toString();
    }
    schema = tableMap.getDatabase();
    table = tableMap.getTable();
    this.row.putAll(row);
    context = new StandardEvaluationContext(this);
  }

  public SyncData(EventType type) {
    this.type = type;
    context = new StandardEvaluationContext(this);
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

  public void addColumn(String colName, Object value) {
    if (value == null) {
      logger.warn("Adding column({}) with null, discarded", colName);
    }
    row.put(colName, value);
  }


  public void renameColumn(String oldKey, String newKey) {
    if (row.containsKey(oldKey)) {
      row.put(newKey, row.get(oldKey));
      row.remove(oldKey);
    } else {
      logger.warn("No such row name: {} in {}.{}", oldKey, schema, table);
    }
  }

  public void removeColumn(String colName) {
    row.remove(colName);
  }

  public void removeColumns(String... colNames) {
    for (String colName : colNames) {
      row.remove(colName);
    }
  }

  public boolean containColumn(String col) {
    return row.containsKey(col);
  }

  public void updateColumn(String column, Object value) {
    if (row.containsKey(column)) {
      row.put(column, value);
    } else {
      logger.warn("No such row name: {} in {}.{}", column, schema, table);
    }
  }

  public StandardEvaluationContext getContext() {
    return context;
  }

  public HashMap<String, Object> getRow() {
    return row;
  }

  public HashMap<String, Object> getExtra() {
    return extra;
  }

  public Object getRowValue(String col) {
    Assert.isTrue(row.containsKey(col), row.toString() + "[No such column]: " + col);
    return row.get(col);
  }

  public HashMap<String, Object> getSyncBy() {
    return syncByQuery.getSyncBy();
  }

  public boolean isSyncWithoutId() {
    return syncByQuery.isSyncWithoutId();
  }

  public SyncByQueryES syncByQuery() {
    return syncByQueryES;
  }

  public ExtraQueryES extraQuery(String indexName, String typeName) {
    // TODO 17/10/23 handle multiple extra query?
    return new ExtraQueryES(this).setIndexName(indexName).setTypeName(typeName);
  }

  /**
   * ----------- update/delete by query -----------
   */
  static class SyncByQueryES {

    private static final Logger logger = LoggerFactory.getLogger(SyncByQueryES.class);

    private final HashMap<String, Object> syncBy = new HashMap<>();

    SyncByQueryES filter(String syncWithCol, String value) {
      syncBy.put(syncWithCol, value);
      return this;
    }

    boolean isSyncWithoutId() {
      if (!syncBy.isEmpty()) {
        return true;
      }
      logger.warn("No filter to use for sync whereas syncWithoutId is set");
      return false;
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

    public ExtraQueryES addColumn(String ... cols) {
      if (cols.length != target.length) {
        throw new InvalidConfigException("Column length is not same as query select result");
      }
      this.cols = cols;
      for (String col : cols) {
        data.getRow().put(col, this);
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
  }

}
