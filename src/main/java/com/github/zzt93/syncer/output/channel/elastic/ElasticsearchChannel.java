package com.github.zzt93.syncer.output.channel.elastic;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.ThreadSafe;
import com.github.zzt93.syncer.config.pipeline.common.ElasticsearchConnection;
import com.github.zzt93.syncer.config.pipeline.output.PipelineBatch;
import com.github.zzt93.syncer.config.pipeline.output.RequestMapping;
import com.github.zzt93.syncer.output.batch.BatchBuffer;
import com.github.zzt93.syncer.output.channel.BufferedChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequestBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class ElasticsearchChannel implements BufferedChannel {

  private final BatchBuffer<WriteRequestBuilder> batchBuffer;
  private final ESRequestMapper esRequestMapper;
  private final Logger logger = LoggerFactory.getLogger(ElasticsearchChannel.class);
  private final TransportClient client;
  private final PipelineBatch batch;

  public ElasticsearchChannel(ElasticsearchConnection connection, RequestMapping requestMapping,
      PipelineBatch batch)
      throws Exception {
    client = connection.transportClient();
    this.batchBuffer = new BatchBuffer<>(batch, WriteRequestBuilder.class);
    this.batch = batch;
    this.esRequestMapper = new ESRequestMapper(client, requestMapping);
  }

  @ThreadSafe(safe = {ESRequestMapper.class, BatchBuffer.class})
  @Override
  public boolean output(SyncData event) {
    Object builder = esRequestMapper.map(event);
    if (builder instanceof WriteRequestBuilder) {
      boolean addRes = batchBuffer.add((WriteRequestBuilder) builder);
      flushIfReachSizeLimit();
      return addRes;
    } else {
      bulkByScrollRequest((AbstractBulkByScrollRequestBuilder) builder);
    }
    return true;
  }

  @Override
  public boolean output(List<SyncData> batch) {
    List<WriteRequestBuilder> collect = batch
        .stream()
        .map(data -> {
          Object builder = esRequestMapper.map(data);
          if (builder instanceof WriteRequestBuilder) {
            return ((WriteRequestBuilder) builder);
          }
          bulkByScrollRequest((AbstractBulkByScrollRequestBuilder) builder);
          return null;
        })
        .filter(Objects::isNull)
        .collect(Collectors.toList());
    boolean addRes = batchBuffer.addAll(collect);
    flushIfReachSizeLimit();
    return addRes;
  }

  private void bulkByScrollRequest(AbstractBulkByScrollRequestBuilder builder) {
    builder.execute(new ActionListener<BulkByScrollResponse>() {
      @Override
      public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
        logger.info("Update/Delete by query: update {} or delete {} documents",
            bulkByScrollResponse.getUpdated(), bulkByScrollResponse.getDeleted());
      }

      @Override
      public void onFailure(Exception e) {
        logger.error("Fail to update/delete by query: {}", builder.request(), e);
      }
    });
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


  @ThreadSafe(safe = {TransportClient.class, BatchBuffer.class})
  @Override
  public void flush() {
    WriteRequestBuilder[] aim = batchBuffer.flush();
    if (aim != null && aim.length != 0) {
      buildRequest(aim);
    }
  }

  @ThreadSafe(safe = {TransportClient.class, BatchBuffer.class})
  private void flushIfReachSizeLimit() {
    WriteRequestBuilder[] aim = batchBuffer.flushIfReachSizeLimit();
    if (aim != null && aim.length != 0) {
      buildRequest(aim);
    }
  }

  private void buildRequest(WriteRequestBuilder[] aim) {
    logger.info("Sending a batch of Elasticsearch: {}", Arrays.toString(aim));
    // TODO 17/10/26 BulkProcessor
    BulkRequestBuilder bulkRequest = client.prepareBulk();
    for (WriteRequestBuilder builder : aim) {
      if (builder instanceof IndexRequestBuilder) {
        bulkRequest.add(((IndexRequestBuilder) builder));
      } else if (builder instanceof UpdateRequestBuilder) {
        bulkRequest.add(((UpdateRequestBuilder) builder));
      } else if (builder instanceof DeleteRequestBuilder) {
        bulkRequest.add(((DeleteRequestBuilder) builder));
      }
    }
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
          "Bulk request has failures. Use ElasticsearchBulkException.getFailedDocuments() for detailed messages ["
              + failedDocuments + "]",
          failedDocuments);
    }
  }
}
