package com.github.zzt93.syncer.consumer.output.channel.elastic;

import com.github.zzt93.syncer.common.data.ExtraQuery;
import com.github.zzt93.syncer.consumer.output.channel.ExtraQueryMapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class ESQueryMapper implements ExtraQueryMapper {

  private final AbstractClient client;
  private final Logger logger = LoggerFactory.getLogger(ESQueryMapper.class);

  public ESQueryMapper(AbstractClient client) {
    this.client = client;
  }

  @Override
  public Map<String, Object> map(ExtraQuery extraQuery) {
    String[] select = extraQuery.getSelect();
    SearchResponse response;
    try {
      response = client.prepareSearch(extraQuery.getIndexName())
          .setTypes(extraQuery.getTypeName())
          .setSearchType(SearchType.DEFAULT)
          .setFetchSource(select, null)
          .setQuery(getFilter(extraQuery))
          .execute()
          .actionGet();
    } catch (Exception e) {
      logger.error("Fail to do the extra query {}", extraQuery, e);
      return Collections.emptyMap();
    }
    SearchHits hits = response.getHits();
    if (hits.totalHits > 1) {
      logger.warn("Multiple query results exists, only use the first");
    } else if (hits.totalHits == 0) {
      logger.warn("Fail to find any match by " + extraQuery);
      return Collections.emptyMap();
    }
    SearchHit hit = hits.getAt(0);
    Map<String, Object> res = new HashMap<>();
    for (int i = 0; i < select.length; i++) {
      res.put(extraQuery.getAs(i), hit.getSource().get(select[i]));
    }
    extraQuery.addQueryResult(res);
    return res;
  }

  private QueryBuilder getFilter(ExtraQuery extraQuery) {
    BoolQueryBuilder bool = new BoolQueryBuilder();
    for (Entry<String, Object> e : extraQuery.getQueryBy().entrySet()) {
      Object value = e.getValue();
      String key = e.getKey();
      Optional<Object> queryResult = getPlaceholderQueryResult(extraQuery, value);
      if (queryResult.isPresent()) {
        extraQuery.filter(key, queryResult.get());
        bool.filter(QueryBuilders.termQuery(key, queryResult.get()));
      } else {
        bool.filter(QueryBuilders.termQuery(key, value));
      }
    }
    return bool;
  }

  private Optional<Object> getPlaceholderQueryResult(ExtraQuery extraQuery, Object value) {
    if (value instanceof String) {
      String str = ((String) value);
      Optional<String> placeholderValue = getPlaceholderValue(str);
      if (placeholderValue.isPresent()) {
        String key = placeholderValue.get();
        Object record = extraQuery.getRecord(key);
        if (!(record instanceof ExtraQuery) || ((ExtraQuery) record).getQueryResult(key) == null) {
          logger.error("Dependent extra query {} has no result of {} from {}", record, key, value);
          return Optional.empty();
        } else {
          return Optional.of(((ExtraQuery) record).getQueryResult(key));
        }
      }
    }
    return Optional.empty();
  }

  /**
   * @param str special placeholder: $userId$
   * means this key is the result of previous query
   */
  private Optional<String> getPlaceholderValue(String str) {
    if (str.startsWith("$") && str.endsWith("$")) {
      return Optional.of(str.substring(1, str.length() - 1));
    }
    return Optional.empty();
  }

}
