package com.github.zzt93.syncer.consumer.output.channel.elastic;

import com.github.zzt93.syncer.common.ExtraQuery;
import com.github.zzt93.syncer.consumer.output.channel.ExtraQueryMapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
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

  private final TransportClient client;
  private final Logger logger = LoggerFactory.getLogger(ESQueryMapper.class);

  public ESQueryMapper(TransportClient client) {
    this.client = client;
  }

  @Override
  public Map<String, Object> map(ExtraQuery extraQuery) {
    String[] select = extraQuery.getSelect();
    SearchResponse response = client.prepareSearch(extraQuery.getIndexName())
        .setTypes(extraQuery.getTypeName())
        .setSearchType(SearchType.DEFAULT)
        .setFetchSource(select, null)
        .setQuery(getFilter(extraQuery.getQueryBy()))
        .execute()
        .actionGet();
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
    return res;
  }

  private QueryBuilder getFilter(HashMap<String, Object> queryBy) {
    BoolQueryBuilder bool = new BoolQueryBuilder();
    for (String s : queryBy.keySet()) {
      bool.filter(QueryBuilders.termQuery(s, queryBy.get(s)));
    }
    return bool;
  }

}
