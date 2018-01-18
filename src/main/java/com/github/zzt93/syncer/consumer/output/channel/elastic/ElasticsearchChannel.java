package com.github.zzt93.syncer.consumer.output.channel.elastic;

import com.github.zzt93.syncer.common.IdGenerator;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncWrapper;
import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.github.zzt93.syncer.config.pipeline.common.ElasticsearchConnection;
import com.github.zzt93.syncer.config.pipeline.output.PipelineBatch;
import com.github.zzt93.syncer.config.pipeline.output.RequestMapping;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.batch.BatchBuffer;
import com.github.zzt93.syncer.consumer.output.channel.BufferedChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequestBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * @author zzt
 */
public class ElasticsearchChannel implements BufferedChannel {

  private final BatchBuffer<SyncWrapper> batchBuffer;
  private final ESRequestMapper esRequestMapper;
  private final Logger logger = LoggerFactory.getLogger(ElasticsearchChannel.class);
  private final TransportClient client;
  private final PipelineBatch batch;
  private final Ack ack;

  public ElasticsearchChannel(ElasticsearchConnection connection, RequestMapping requestMapping,
      PipelineBatch batch, Ack ack)
      throws Exception {
    client = connection.transportClient();
    this.batchBuffer = new BatchBuffer<>(batch, SyncWrapper.class);
    this.batch = batch;
    this.esRequestMapper = new ESRequestMapper(client, requestMapping);
    this.ack = ack;
  }

  public static String toString(UpdateRequest request) {
    String res = "update {[" + request.index() + "][" + request.type() + "][" + request.id() + "]";
    if (request.docAsUpsert()) {
      res += (", doc_as_upsert[" + request.docAsUpsert() + "]");
    }
    if (request.doc() != null) {
      res += (", doc[" + request.doc() + "]");
    }
    if (request.script() != null) {
      res += (", script[" + request.script() + "]");
    }
    if (request.upsertRequest() != null) {
      res += (", upsert[" + request.upsertRequest() + "]");
    }
    if (request.scriptedUpsert()) {
      res += (", scripted_upsert[" + request.scriptedUpsert() + "]");
    }
    if (request.detectNoop()) {
      res += (", detect_noop[" + request.detectNoop() + "]");
    }
    if (request.fields() != null) {
      res += (", fields[" + Arrays.toString(request.fields()) + "]");
    }
    return res + "}";
  }

  @ThreadSafe(safe = {ESRequestMapper.class, BatchBuffer.class})
  @Override
  public boolean output(SyncData event) {
    Object builder = esRequestMapper.map(event);
    if (builder instanceof WriteRequestBuilder) {
      boolean addRes = batchBuffer.add(
          new SyncWrapper<>(event, (WriteRequestBuilder) builder));
      flushIfReachSizeLimit();
      return addRes;
    } else {
      bulkByScrollRequest(event, (AbstractBulkByScrollRequestBuilder) builder);
    }
    return true;
  }

  private void bulkByScrollRequest(SyncData data, AbstractBulkByScrollRequestBuilder builder) {
    builder.execute(new ActionListener<BulkByScrollResponse>() {
      @Override
      public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
        MDC.put(IdGenerator.EID, data.getEventId());
        logger.info("Update/Delete by query {}: update {} or delete {} documents",
            builder.request(), bulkByScrollResponse.getUpdated(),
            bulkByScrollResponse.getDeleted());
        ack.remove(data.getSourceIdentifier(), data.getDataId());
      }

      @Override
      public void onFailure(Exception e) {
        MDC.put(IdGenerator.EID, data.getEventId());
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
    SyncWrapper<WriteRequestBuilder>[] aim = batchBuffer.flush();
    buildAndSend(aim);
  }

  private void ackSuccess(SyncWrapper<WriteRequestBuilder>[] aim) {
    for (SyncWrapper<WriteRequestBuilder> wrapper : aim) {
      ack.remove(wrapper.getSourceId(), wrapper.getSyncDataId());
    }
  }

  private void retryFailedDoc(SyncWrapper<WriteRequestBuilder>[] aim,
      ElasticsearchBulkException e) {
    Map<String, String> failedDocuments = e.getFailedDocuments();
    for (SyncWrapper<WriteRequestBuilder> wrapper : aim) {
      WriteRequestBuilder builder = wrapper.getData();
      WriteRequest request = builder.request();
      String id, reqStr = null;
      if (request instanceof IndexRequest) {
        id = ((IndexRequest) request).id();
      } else if (request instanceof UpdateRequest) {
        id = ((UpdateRequest) request).id();
        reqStr = toString((UpdateRequest) request);
      } else if (request instanceof DeleteRequest) {
        id = ((DeleteRequest) request).id();
      } else {
        throw new IllegalStateException("Impossible: " + request);
      }
      if (failedDocuments.containsKey(id)) {
        logger.info("Retry request: {}", reqStr == null ? request : reqStr);
        if (wrapper.count() < batch.getMaxRetry()) {
          batchBuffer.addFirst(wrapper);
        } else {
          // TODO 18/1/18 fail log
          logger.error("Max retry exceed, write to fail.log");
        }
      } else {
        ack.remove(wrapper.getSourceId(), wrapper.getSyncDataId());
      }
    }
  }

  @ThreadSafe(safe = {TransportClient.class, BatchBuffer.class})
  @Override
  public void flushIfReachSizeLimit() {
    SyncWrapper<WriteRequestBuilder>[] aim = batchBuffer.flushIfReachSizeLimit();
    buildAndSend(aim);
  }

  private void buildAndSend(SyncWrapper<WriteRequestBuilder>[] aim) {
    if (aim != null && aim.length != 0) {
      try {
        buildRequest(aim);
        ackSuccess(aim);
      } catch (ElasticsearchBulkException e) {
        retryFailedDoc(aim, e);
      }
    }
  }

  private void buildRequest(SyncWrapper<WriteRequestBuilder>[] aim) {
    StringJoiner joiner = new StringJoiner(",", "[", "]");
    // TODO 17/10/26 BulkProcessor
    BulkRequestBuilder bulkRequest = client.prepareBulk();
    for (SyncWrapper<WriteRequestBuilder> wrapper : aim) {
      WriteRequestBuilder builder = wrapper.getData();
      if (builder instanceof IndexRequestBuilder) {
        joiner.add(builder.request().toString());
        bulkRequest.add(((IndexRequestBuilder) builder));
      } else if (builder instanceof UpdateRequestBuilder) {
        joiner.add(toString(((UpdateRequestBuilder) builder).request()));
        bulkRequest.add(((UpdateRequestBuilder) builder));
      } else if (builder instanceof DeleteRequestBuilder) {
        joiner.add(builder.request().toString());
        bulkRequest.add(((DeleteRequestBuilder) builder));
      }
    }
    logger.info("Sending a batch of Elasticsearch: {}", joiner);
    checkForBulkUpdateFailure(bulkRequest.execute().actionGet());
  }

  private void checkForBulkUpdateFailure(BulkResponse bulkResponse) {
    if (bulkResponse.hasFailures()) {
      Map<String, String> failedDocuments = new HashMap<>();
      for (BulkItemResponse item : bulkResponse.getItems()) {
        if (item.isFailed()) {
          failedDocuments.put(item.getId(), item.getFailure().toString());
        }
      }
      throw new ElasticsearchBulkException("Bulk request has failures: [" + failedDocuments + "]",
          failedDocuments);
    }
  }
}
