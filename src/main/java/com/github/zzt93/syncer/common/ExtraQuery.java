package com.github.zzt93.syncer.common;

import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import java.util.Arrays;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ----------- add separated query before index ------------
 */
public class ExtraQuery {

  private static final Logger logger = LoggerFactory.getLogger(ExtraQuery.class);
  private final HashMap<String, Object> queryBy = new HashMap<>();
  private final SyncData data;
  private String queryId;
  private String indexName;
  private String typeName;
  private String[] target;
  private String[] cols;

  ExtraQuery(SyncData data) {
    this.data = data;
  }

  public String getTypeName() {
    return typeName;
  }

  ExtraQuery setTypeName(String typeName) {
    this.typeName = typeName;
    return this;
  }

  public ExtraQuery filter(String field, Object value) {
    queryBy.put(field, value);
    return this;
  }

  public ExtraQuery select(String... field) {
    target = field;
    return this;
  }

  public ExtraQuery addRecord(String... cols) {
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

  ExtraQuery setIndexName(String indexName) {
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
    return "ExtraQuery{select " + Arrays.toString(target) + " as " + Arrays.toString(cols)
        + " from " + indexName + "." + typeName + " where " + queryBy +"}";
  }
}
