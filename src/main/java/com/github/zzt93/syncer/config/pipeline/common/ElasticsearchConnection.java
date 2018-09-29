package com.github.zzt93.syncer.config.pipeline.common;

import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
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
   * @see org.elasticsearch.transport.TcpTransport#TCP_CONNECT_TIMEOUT
   */
  public AbstractClient esClient() throws Exception {
    // https://discuss.elastic.co/t/getting-availableprocessors-is-already-set-to-1-rejecting-1-illegalstateexception-exception/103082
    System.setProperty("es.set.netty.runtime.available.processors", "false");

    TransportClient client = new PreBuiltXPackTransportClient(settings());
//    TransportClient client = new PreBuiltTransportClient(settings());
    for (String clusterNode : getClusterNodes()) {
      String hostName = substringBeforeLast(clusterNode, COLON);
      String port = substringAfterLast(clusterNode, COLON);
      Assert.hasText(hostName, "[Assertion failed] missing host name in 'clusterNodes'");
      Assert.hasText(port, "[Assertion failed] missing port in 'clusterNodes'");
      logger.info("Adding transport node : {}, timeout in 30s", clusterNode);
      client.addTransportAddress(
          new InetSocketTransportAddress(InetAddress.getByName(hostName), Integer.valueOf(port)));
    }
    return client;
  }

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

}
