package com.github.zzt93.syncer.config.pipeline.output;

import com.github.zzt93.syncer.config.pipeline.common.ElasticsearchConnection;
import com.github.zzt93.syncer.output.OutputChannel;
import com.github.zzt93.syncer.output.elastic.ElasticsearchChannel;

/**
 * @author zzt
 */
public class Elasticsearch implements OutputChannelConfig {

  private ElasticsearchConnection connection;
  private DocumentMapping documentMapping;
  private PipelineBatch batch = new PipelineBatch();

  public ElasticsearchConnection getConnection() {
    return connection;
  }

  public void setConnection(ElasticsearchConnection connection) {
    this.connection = connection;
  }

  public DocumentMapping getDocumentMapping() {
    return documentMapping;
  }

  public void setDocumentMapping(DocumentMapping documentMapping) {
    this.documentMapping = documentMapping;
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
      return new ElasticsearchChannel(connection, documentMapping, batch);
    }
    throw new IllegalArgumentException();
  }
}
