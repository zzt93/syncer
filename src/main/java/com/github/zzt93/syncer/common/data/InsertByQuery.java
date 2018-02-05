package com.github.zzt93.syncer.common.data;

import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import java.util.Arrays;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ----------- index/insert by query ------------
 * @see SyncByQuery
 */
public class InsertByQuery {

  private static final Logger logger = LoggerFactory.getLogger(InsertByQuery.class);
  private final HashMap<String, Object> queryBy = new HashMap<>();
  private final SyncData data;
  private String queryId;
  private String indexName;
  private String typeName;
  private String[] select;
  private String[] as;

  InsertByQuery(SyncData data) {
    this.data = data;
  }

  public String getTypeName() {
    return typeName;
  }

  InsertByQuery setTypeName(String typeName) {
    this.typeName = typeName;
    return this;
  }

  public InsertByQuery filter(String field, Object value) {
    queryBy.put(field, value);
    return this;
  }

  public InsertByQuery select(String... field) {
    select = field;
    return this;
  }

  public InsertByQuery addRecord(String... cols) {
    if (cols.length != select.length) {
      throw new InvalidConfigException("Column length is not same as query select result");
    }
    this.as = cols;
    for (String col : cols) {
      data.getRecords().put(col, this);
    }
    return this;
  }

  public String getIndexName() {
    return indexName;
  }

  InsertByQuery setIndexName(String indexName) {
    this.indexName = indexName;
    return this;
  }

  public HashMap<String, Object> getQueryBy() {
    return queryBy;
  }

  public String[] getSelect() {
    return select;
  }

  public String getAs(int i) {
    return as[i];
  }

  @Override
  public String toString() {
    return "InsertByQuery{select " + Arrays.toString(select) + " as " + Arrays.toString(as)
        + " from " + indexName + "." + typeName + " where " + queryBy +"}";
  }
}
