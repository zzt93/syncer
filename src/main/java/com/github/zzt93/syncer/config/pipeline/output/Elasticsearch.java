package com.github.zzt93.syncer.config.pipeline.output;

import com.github.zzt93.syncer.config.pipeline.common.ElasticsearchConnection;
import com.github.zzt93.syncer.output.ElasticsearchChannel;
import com.github.zzt93.syncer.output.OutputChannel;

/**
 * @author zzt
 */
public class Elasticsearch implements OutputChannelConfig {

  private ElasticsearchConnection connection;
  private DocumentMapping documentMapping;

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

  @Override
  public OutputChannel toChannel() throws Exception {
    if (connection.valid()) {
      return new ElasticsearchChannel(connection, documentMapping);
    }
    throw new IllegalArgumentException();
  }
}
