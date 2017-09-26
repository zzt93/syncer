package com.github.zzt93.syncer.output.channel.elastic;

import java.util.Map;

/**
 * Created by zzt on 9/24/17.
 *
 * <h3></h3>
 */
public class ElasticsearchBulkException extends RuntimeException {

  private final Map<String, String> failedDocuments;

  public ElasticsearchBulkException(String msg, Map<String, String> failedDocuments) {
    super(msg);
    this.failedDocuments = failedDocuments;
  }
}
