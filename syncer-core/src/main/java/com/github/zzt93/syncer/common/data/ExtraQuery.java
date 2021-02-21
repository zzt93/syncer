package com.github.zzt93.syncer.common.data;

import com.github.zzt93.syncer.config.common.InvalidConfigException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Write/Update/Delete by query <em>Elasticsearch output channel</em>
 * @see SyncByQuery
 */
public class ExtraQuery implements com.github.zzt93.syncer.data.ExtraQuery {

  private static final Logger logger = LoggerFactory.getLogger(ExtraQuery.class);
  private static final String ES_ID = "_id";
  private final HashMap<String, Object> queryBy = new HashMap<>();
  private final transient SyncData data;
  private String indexName;
  private String typeName;
  private String[] select;
  private String[] as;
  private final HashMap<String, Object> queryResult = new HashMap<>();

  ExtraQuery(SyncData data) {
    this.data = data;
  }

  public String getTypeName() {
    return typeName;
  }

  public ExtraQuery setTypeName(String typeName) {
    this.typeName = typeName;
    return this;
  }

  public ExtraQuery eq(String name, Object value) {
    queryBy.put(name, value);
    return this;
  }

  @Override
  public ExtraQuery filter(String name, Object value) {
    return eq(name, value);
  }

  @Override
  public com.github.zzt93.syncer.data.ExtraQuery id(Object value) {
    return eq(ES_ID, value);
  }

  public ExtraQuery select(String... field) {
    select = field;
    for (String col : field) {
      data.getFields().put(col, new ExtraQueryField(this, col));
    }
    return this;
  }

  public ExtraQuery as(String... cols) {
    if (cols.length != select.length) {
      throw new InvalidConfigException("Column length is not same as query select result");
    }
    for (String s : select) {
      data.removeField(s);
    }
    this.as = cols;
    for (String col : cols) {
      data.addField(col, new ExtraQueryField(this, col));
    }
    return this;
  }

  @Override
  public com.github.zzt93.syncer.data.ExtraQuery addField(String... cols) {
    return as(cols);
  }

  public String getIndexName() {
    return indexName;
  }

  public ExtraQuery setIndexName(String indexName) {
    this.indexName = indexName;
    return this;
  }

  private HashMap<String, Object> getQueryBy() {
    return queryBy;
  }

  public String[] getSelect() {
    return select;
  }

  public String getAs(int i) {
    return as != null ? as[i] : select[i];
  }

  public void addQueryResult(Map<String, Object> result) {
    queryResult.putAll(result);
  }

  public Object getQueryResult(String key) {
    Object o = queryResult.get(key);
    if (o == null) {
      logger.warn("Fail to query [{}] by {}", key, this);
    }
    return o;
  }

  @Override
  public String toString() {
    return "ExtraQuery{select " + Arrays.toString(select) + " as " + Arrays.toString(as)
        + " from " + indexName + "." + typeName + " where " + queryBy +"}" + (!queryResult.isEmpty() ? queryResult : "");
  }

  public Optional<QueryBuilder> getEsFilter() {
    BoolQueryBuilder bool = new BoolQueryBuilder();
    boolean hasCondition = false;
    for (Map.Entry<String, Object> e : getQueryBy().entrySet()) {
      Object value = e.getValue();
      String key = e.getKey();
      Optional<Object> realValue = getRealValue(value);
      if (realValue.isPresent()) {
        eq(key, realValue.get());
        bool.filter(QueryBuilders.termQuery(key, realValue.get()));
        hasCondition = true;
      }
    }
    return hasCondition ? Optional.of(bool) : Optional.empty();
  }

  private Optional<Object> getRealValue(Object value) {
    if (value instanceof ExtraQueryField) {
      ExtraQueryField extraQueryField = ((ExtraQueryField) value);
      if (extraQueryField.getQueryResult() == null) {
        logger.error("Dependent extra query has no result {}", value);
        return Optional.empty();
      }
      return Optional.of(extraQueryField.getQueryResult());
    }
    return Optional.of(value);
  }
}
