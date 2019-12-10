package com.github.zzt93.syncer.consumer.output.channel.elastic;

import com.github.zzt93.syncer.config.common.ElasticsearchConnection;
import com.google.common.collect.Lists;
import org.elasticsearch.client.support.AbstractClient;

/**
 * @author zzt
 */
public class ElasticTestUtil {

  public static AbstractClient getIntClient() throws Exception {
    ElasticsearchConnection elasticsearchConnection = new ElasticsearchConnection();
    elasticsearchConnection.setClusterName("searcher-integration");
    elasticsearchConnection.setClusterNodes(Lists.newArrayList("192.168.1.100:9300"));

    return elasticsearchConnection.esClient();
  }
  public static AbstractClient getDevClient() throws Exception {
    ElasticsearchConnection elasticsearchConnection = new ElasticsearchConnection();
    elasticsearchConnection.setClusterName("searcher-dev");
    elasticsearchConnection.setClusterNodes(Lists.newArrayList("192.168.1.204:9300"));

    return elasticsearchConnection.esClient();
  }
}
