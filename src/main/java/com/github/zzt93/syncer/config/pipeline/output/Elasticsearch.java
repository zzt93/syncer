package com.github.zzt93.syncer.config.pipeline.output;

import com.github.zzt93.syncer.config.pipeline.common.ElasticsearchConnection;

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

}
