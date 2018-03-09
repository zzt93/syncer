package com.github.zzt93.syncer.config.pipeline.common;

import static org.apache.commons.lang.StringUtils.split;
import static org.apache.commons.lang.StringUtils.substringAfterLast;
import static org.apache.commons.lang.StringUtils.substringBeforeLast;

import java.net.InetAddress;
import java.util.stream.Collectors;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
public class ElasticsearchConnection extends ClusterConnection {

  private final Logger logger = LoggerFactory.getLogger(ElasticsearchConnection.class);

  public TransportClient transportClient() throws Exception {
    ElasticsearchConnection elasticsearch = this;
    PreBuiltXPackTransportClient client = new PreBuiltXPackTransportClient(
        elasticsearch.settings());
    String clusterNodes = elasticsearch.clusterNodesString();
    Assert.hasText(clusterNodes, "[Assertion failed] clusterNodes settings missing.");
    for (String clusterNode : split(clusterNodes, COMMA)) {
      String hostName = substringBeforeLast(clusterNode, COLON);
      String port = substringAfterLast(clusterNode, COLON);
      Assert.hasText(hostName, "[Assertion failed] missing host name in 'clusterNodes'");
      Assert.hasText(port, "[Assertion failed] missing port in 'clusterNodes'");
      logger.info("Adding transport node : {}", clusterNode);
      client.addTransportAddress(
          new InetSocketTransportAddress(InetAddress.getByName(hostName), Integer.valueOf(port)));
    }
    return client;
  }

  private String clusterNodesString() {
    return getClusterNodes().stream().collect(Collectors.joining(","));
  }

  private Settings settings() {
    Builder builder = Settings.builder()
        .put("cluster.name", getClusterName());
    if (getUser() == null && getPassword() == null) {
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

}
