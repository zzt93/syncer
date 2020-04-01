package com.github.zzt93.syncer.consumer.output.channel.elastic;

import com.github.zzt93.syncer.config.common.ElasticsearchConnection;
import com.google.common.collect.Lists;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * @author zzt
 */
public class ElasticTestUtil {

  public static RestHighLevelClient getIntClient() throws Exception {
    ElasticsearchConnection elasticsearchConnection = new ElasticsearchConnection();
    elasticsearchConnection.setClusterName("searcher-integration");
    elasticsearchConnection.setClusterNodes(Lists.newArrayList("192.168.1.100:9200"));

    return elasticsearchConnection.esClient();
  }
  public static RestHighLevelClient getDevClient() throws Exception {
    ElasticsearchConnection elasticsearchConnection = new ElasticsearchConnection();
    elasticsearchConnection.setClusterName("searcher-dev");
    elasticsearchConnection.setClusterNodes(Lists.newArrayList("192.168.1.202:9200"));

    return elasticsearchConnection.esClient();
  }
}
