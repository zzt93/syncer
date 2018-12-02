package com.github.zzt93.syncer.consumer.output.channel.elastic;

import org.elasticsearch.action.bulk.BulkResponse;

/**
 * Created by zzt on 9/24/17.
 *
 * <h3></h3>
 */
public class ElasticsearchBulkException extends RuntimeException {

  private final BulkResponse bulkItemResponses;

  ElasticsearchBulkException(String msg, BulkResponse bulkItemResponses) {
    super(msg);
    this.bulkItemResponses = bulkItemResponses;
  }

  public BulkResponse getBulkItemResponses() {
    return bulkItemResponses;
  }
}
