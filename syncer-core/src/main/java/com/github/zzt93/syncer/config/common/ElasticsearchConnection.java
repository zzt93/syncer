package com.github.zzt93.syncer.config.common;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.net.InetAddress;

import static org.apache.commons.lang.StringUtils.substringAfterLast;
import static org.apache.commons.lang.StringUtils.substringBeforeLast;

/**
 * Created by zzt on 9/11/17. <h3></h3>
 */
public class ElasticsearchConnection extends ClusterConnection {

  private final Logger logger = LoggerFactory.getLogger(ElasticsearchConnection.class);

  /**
   */
  public RestHighLevelClient esClient() throws Exception {
    // https://discuss.elastic.co/t/getting-availableprocessors-is-already-set-to-1-rejecting-1-illegalstateexception-exception/103082
    System.setProperty("es.set.netty.runtime.available.processors", "false");

    HttpHost[] host = new HttpHost[getClusterNodes().size()];
    for (int i = 0; i < getClusterNodes().size(); i++) {
      String clusterNode = getClusterNodes().get(i);
      String hostName = substringBeforeLast(clusterNode, COLON);
      String port = substringAfterLast(clusterNode, COLON);
      Assert.hasText(hostName, "[Assertion failed] missing host name in 'clusterNodes'");
      Assert.hasText(port, "[Assertion failed] missing port in 'clusterNodes'");
      logger.info("Adding transport node : {}, timeout in 30s", clusterNode);
      host[i] = new HttpHost(InetAddress.getByName(hostName), Integer.valueOf(port));
    }
    return new RestHighLevelClient(
        RestClient.builder(host)
            .setFailureListener(new RestClient.FailureListener() {
              @Override
              public void onFailure(Node node) {
                logger.error("Fail to connect {}", node);
              }
            })
            .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                .setDefaultCredentialsProvider(credentialsProvider()))
    );
  }

  private CredentialsProvider credentialsProvider() {
    final CredentialsProvider credentialsProvider =
        new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials("user", "password"));
    return credentialsProvider;
  }

  // TODO 2019-10-19 cluster name?
  private Settings settings() {
    Builder builder = Settings.builder()
        .put("cluster.name", getClusterName());
    if (getUser() == null && noPassword()) {
      return builder.build();
    }
    if (getUser() == null || getPassword() == null) {
      throw new IllegalArgumentException("Lacking user or password");
    }
    return builder
        .put("xpack.security.user", getUser() + COLON + getPassword())
        //        .put("client.transport.sniff", clientTransportSniff)
        //        .put("client.transport.ignore_cluster_name", clientIgnoreClusterName)
        //        .put("client.transport.ping_timeout", clientPingTimeout)
        //        .put("client.transport.nodes_sampler_interval", clientNodesSamplerInterval)
        .build();
  }

  @Override
  public boolean valid() {
    return !StringUtils.isEmpty(getClusterName()) && !getClusterNodes().isEmpty();
  }

}
