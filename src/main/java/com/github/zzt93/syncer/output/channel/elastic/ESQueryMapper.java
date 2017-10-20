package com.github.zzt93.syncer.output.channel.elastic;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.output.mapper.Mapper;
import java.util.Map;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

/**
 * @author zzt
 */
public class ESQueryMapper implements Mapper<SyncData, Map<String, Object>> {
  private final TransportClient client;

  public ESQueryMapper(TransportClient client) {
    this.client = client;
  }

  @Override
  public Map<String, Object> map(SyncData data) {
    SearchResponse response = client.prepareSearch("my-index")
        .setTypes("my-type")
        .setSearchType(SearchType.QUERY_AND_FETCH)
        .setFetchSource(new String[]{"field1"}, null)
        .setQuery(QueryBuilders.termsQuery("field1", "1234"))
        .execute()
        .actionGet();
    for (SearchHit searchHitFields : response.getHits()) {

    }
    return null;
  }
}
