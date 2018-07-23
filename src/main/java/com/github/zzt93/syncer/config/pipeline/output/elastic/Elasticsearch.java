package com.github.zzt93.syncer.config.pipeline.output.elastic;

import com.github.zzt93.syncer.config.pipeline.common.ElasticsearchConnection;
import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import com.github.zzt93.syncer.config.pipeline.output.FailureLogConfig;
import com.github.zzt93.syncer.config.pipeline.output.OutputChannelConfig;
import com.github.zzt93.syncer.config.pipeline.output.PipelineBatch;
import com.github.zzt93.syncer.config.syncer.SyncerOutputMeta;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.elastic.ElasticsearchChannel;
import com.google.common.base.Preconditions;

/**
 * @author zzt
 */
public class Elasticsearch implements OutputChannelConfig {

  private ElasticsearchConnection connection;
  private ESRequestMapping requestMapping = new ESRequestMapping();
  private PipelineBatch batch = new PipelineBatch();
  private FailureLogConfig failureLog = new FailureLogConfig();
  private long refreshInMillis = 0;

  public FailureLogConfig getFailureLog() {
    return failureLog;
  }

  public void setFailureLog(FailureLogConfig failureLog) {
    this.failureLog = failureLog;
  }

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

  public PipelineBatch getBatch() {
    return batch;
  }

  public void setBatch(PipelineBatch batch) {
    this.batch = batch;
  }

  @Override
  public ElasticsearchChannel toChannel(Ack ack,
      SyncerOutputMeta outputMeta) throws Exception {
    if (connection.valid()) {
      return new ElasticsearchChannel(this, outputMeta, ack);
    }
    throw new InvalidConfigException("Invalid connection configuration: " + connection);
  }
}
