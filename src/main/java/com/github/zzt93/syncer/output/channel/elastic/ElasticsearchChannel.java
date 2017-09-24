package com.github.zzt93.syncer.output.channel.elastic;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.config.pipeline.common.ElasticsearchConnection;
import com.github.zzt93.syncer.config.pipeline.output.DocumentMapping;
import com.github.zzt93.syncer.config.pipeline.output.PipelineBatch;
import com.github.zzt93.syncer.output.batch.BatchBuffer;
import com.github.zzt93.syncer.output.channel.BufferedChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.elasticsearch.ElasticsearchException;

/**
 * @author zzt
 */
public class ElasticsearchChannel implements BufferedChannel {

  private final BatchBuffer<WriteRequestBuilder> batchBuffer;
  private final ESDocumentMapper esDocumentMapper;
  private final Logger logger = LoggerFactory.getLogger(ElasticsearchChannel.class);
  private final TransportClient client;
  private final PipelineBatch batch;

  public ElasticsearchChannel(ElasticsearchConnection connection, DocumentMapping documentMapping,
      PipelineBatch batch)
      throws Exception {
    client = connection.transportClient();
    this.batchBuffer = new BatchBuffer<>(batch);
    this.batch = batch;
    this.esDocumentMapper = new ESDocumentMapper(documentMapping, client);
  }

  @Override
  public boolean output(SyncData event) {
    return batchBuffer.add(esDocumentMapper.map(event));
  }

  @Override
  public boolean output(List<SyncData> batch) {
    List<WriteRequestBuilder> collect = batch.stream().map(esDocumentMapper::map)
        .collect(Collectors.toList());
    return batchBuffer.addAll(collect);
  }

  @Override
  public String des() {
    return "ElasticsearchChannel{" +
        "esTemplate=" + client +
        '}';
  }

  @Override
  public long getDelay() {
    return batch.getDelay();
  }

  @Override
  public TimeUnit getDelayUnit() {
    return batch.getDelayTimeUnit();
  }

  @Override
  public void flush() {
    BulkRequestBuilder bulkRequest = client.prepareBulk();
    batchBuffer.flush();
    checkForBulkUpdateFailure(bulkRequest.execute().actionGet());
  }

  private void checkForBulkUpdateFailure(BulkResponse bulkResponse) {
    if (bulkResponse.hasFailures()) {
      Map<String, String> failedDocuments = new HashMap<>();
      for (BulkItemResponse item : bulkResponse.getItems()) {
        if (item.isFailed()) {
          failedDocuments.put(item.getId(), item.getFailureMessage());
        }
      }
      throw new ElasticsearchBulkException(
          "Bulk indexing has failures. Use ElasticsearchBulkException.getFailedDocuments() for detailed messages ["
              + failedDocuments + "]",
          failedDocuments);
    }
  }
}
