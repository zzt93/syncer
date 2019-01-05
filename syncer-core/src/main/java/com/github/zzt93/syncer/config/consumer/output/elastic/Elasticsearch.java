package com.github.zzt93.syncer.config.consumer.output.elastic;

import com.github.zzt93.syncer.config.common.ElasticsearchConnection;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.consumer.output.BufferedOutputChannelConfig;
import com.github.zzt93.syncer.config.syncer.SyncerOutputMeta;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.elastic.ElasticsearchChannel;
import com.google.common.base.Preconditions;
import org.springframework.util.StringUtils;

/**
 * @author zzt
 */
public class Elasticsearch extends BufferedOutputChannelConfig {

  private ElasticsearchConnection connection;
  private ESRequestMapping requestMapping = new ESRequestMapping();
  private long refreshInMillis = 1000;

  public ElasticsearchConnection getConnection() {
    return connection;
  }

  public void setConnection(ElasticsearchConnection connection) {
    this.connection = connection;
  }

  public ESRequestMapping getRequestMapping() {
    return requestMapping;
  }

  public void setRequestMapping(ESRequestMapping requestMapping) {
    this.requestMapping = requestMapping;
  }

  public long getRefreshInMillis() {
    return refreshInMillis;
  }

  public void setRefreshInMillis(long refreshInMillis) {
    Preconditions.checkArgument(refreshInMillis >= 0, "Invalid [refreshInMillis] config");
    this.refreshInMillis = refreshInMillis;
  }

  private String consumerId;

  @Override
  public String getConsumerId() {
    return consumerId;
  }

  @Override
  public ElasticsearchChannel toChannel(String consumerId, Ack ack,
      SyncerOutputMeta outputMeta) throws Exception {
    this.consumerId = consumerId;
    if (!StringUtils.isEmpty(connection) && connection.valid()) {
      return new ElasticsearchChannel(this, outputMeta, ack);
    }
    throw new InvalidConfigException("Invalid connection configuration: " + connection);
  }
}
