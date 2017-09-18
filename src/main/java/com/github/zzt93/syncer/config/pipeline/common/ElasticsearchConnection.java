package com.github.zzt93.syncer.config.pipeline.common;

import static org.apache.commons.lang.StringUtils.split;
import static org.apache.commons.lang.StringUtils.substringAfterLast;
import static org.apache.commons.lang.StringUtils.substringBeforeLast;

import com.github.zzt93.syncer.config.pipeline.PipelineConfig;
import java.net.InetAddress;
import java.util.List;
import java.util.stream.Collectors;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
@Configuration
public class ElasticsearchConnection extends Connection {

  private static final Logger logger = LoggerFactory.getLogger(ElasticsearchConnection.class);
  private static final String COMMA = ",";
  private static final String COLON = ":";
  private String clusterName = "elasticsearch";
  private List<String> clusterNodes;

  @Bean
  @ConditionalOnProperty(prefix = "pipeline.output.elasticsearch.connection", name = {"cluster-name",
      "cluster-nodes[0]"})
  public static TransportClient transportClient(PipelineConfig pipelineConfig) throws Exception {
    ElasticsearchConnection elasticsearch = pipelineConfig.getOutput().getElasticsearch()
        .getConnection();
    Assert.notNull(elasticsearch, "[Assertion failed] ");
    PreBuiltXPackTransportClient client = new PreBuiltXPackTransportClient(
        elasticsearch.settings());
    String clusterNodes = elasticsearch.clusterNodesString();
    Assert.hasText(clusterNodes, "[Assertion failed] clusterNodes settings missing.");
    for (String clusterNode : split(clusterNodes, COMMA)) {
      String hostName = substringBeforeLast(clusterNode, COLON);
      String port = substringAfterLast(clusterNode, COLON);
      Assert.hasText(hostName, "[Assertion failed] missing host name in 'clusterNodes'");
      Assert.hasText(port, "[Assertion failed] missing port in 'clusterNodes'");
      logger.info("adding transport node : {}", clusterNode);
      client.addTransportAddress(
          new InetSocketTransportAddress(InetAddress.getByName(hostName), Integer.valueOf(port)));
    }
    return client;
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public List<String> getClusterNodes() {
    return clusterNodes;
  }

  public void setClusterNodes(List<String> clusterNodes) {
    this.clusterNodes = clusterNodes;
  }

  private String clusterNodesString() {
    return clusterNodes.stream().collect(Collectors.joining(","));
  }

  private Settings settings() {
    return Settings.builder()
        .put("cluster.name", getClusterName())
        .put("xpack.security.user", getUser() + COLON + getPassword())
        //        .put("client.transport.sniff", clientTransportSniff)
        //        .put("client.transport.ignore_cluster_name", clientIgnoreClusterName)
        //        .put("client.transport.ping_timeout", clientPingTimeout)
        //        .put("client.transport.nodes_sampler_interval", clientNodesSamplerInterval)
        .build();
  }

}
