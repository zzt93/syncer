package com.github.zzt93.syncer.config.pipeline.output;

import com.github.zzt93.syncer.config.pipeline.common.ElasticsearchConnection;
import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;
import com.github.zzt93.syncer.consumer.output.channel.elastic.ElasticsearchChannel;

/**
 * @author zzt
 */
public class Elasticsearch implements OutputChannelConfig {

  private ElasticsearchConnection connection;
  private RequestMapping requestMapping = new RequestMapping();
  private PipelineBatch batch = new PipelineBatch();

  public ElasticsearchConnection getConnection() {
    return connection;
  }

  public void setConnection(ElasticsearchConnection connection) {
    this.connection = connection;
  }

  public RequestMapping getRequestMapping() {
    return requestMapping;
  }

  public void setRequestMapping(RequestMapping requestMapping) {
    this.requestMapping = requestMapping;
  }


  public PipelineBatch getBatch() {
    return batch;
  }

  public void setBatch(PipelineBatch batch) {
    this.batch = batch;
  }

  @Override
  public OutputChannel toChannel() throws Exception {
    if (connection.valid()) {
      return new ElasticsearchChannel(connection, requestMapping, batch);
    }
    throw new InvalidConfigException("Invalid connection configuration: " + connection);
  }
}
