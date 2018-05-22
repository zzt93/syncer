package com.github.zzt93.syncer.consumer.output.channel.elastic;

import com.github.zzt93.syncer.common.IdGenerator;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncWrapper;
import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.github.zzt93.syncer.config.pipeline.common.ElasticsearchConnection;
import com.github.zzt93.syncer.config.pipeline.output.FailureLogConfig;
import com.github.zzt93.syncer.config.pipeline.output.PipelineBatch;
import com.github.zzt93.syncer.config.pipeline.output.elastic.Elasticsearch;
import com.github.zzt93.syncer.config.syncer.SyncerOutputMeta;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.ack.FailureEntry;
import com.github.zzt93.syncer.consumer.ack.FailureLog;
import com.github.zzt93.syncer.consumer.output.batch.BatchBuffer;
import com.github.zzt93.syncer.consumer.output.channel.BufferedChannel;
import com.google.gson.reflect.TypeToken;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequestBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * @author zzt
 */
public class ElasticsearchChannel implements BufferedChannel<WriteRequest> {

  private long refreshInterval = 1000;
  private final BatchBuffer<SyncWrapper<WriteRequest>> batchBuffer;
  private final ESRequestMapper esRequestMapper;
  private final Logger logger = LoggerFactory.getLogger(ElasticsearchChannel.class);
  private final TransportClient client;
  private final PipelineBatch batch;
  private final Ack ack;
  private final FailureLog<SyncData> singleRequest;

  public ElasticsearchChannel(Elasticsearch elasticsearch, SyncerOutputMeta outputMeta, Ack ack)
      throws Exception {
    ElasticsearchConnection connection = elasticsearch.getConnection();
    client = connection.transportClient();
    refreshInterval = elasticsearch.getRefreshInMillis();
    this.batchBuffer = new BatchBuffer<>(elasticsearch.getBatch());
    this.batch = elasticsearch.getBatch();
    this.esRequestMapper = new ESRequestMapper(client, elasticsearch.getRequestMapping());
    this.ack = ack;
    FailureLogConfig failureLog = elasticsearch.getFailureLog();
    try {
      Path path = Paths.get(outputMeta.getFailureLogDir(), connection.connectionIdentifier());
      singleRequest = new FailureLog<>(path,
          failureLog, new TypeToken<FailureEntry<SyncData>>() {
      });
    } catch (FileNotFoundException e) {
      throw new IllegalStateException("Impossible", e);
    }
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
    // TODO 18/3/25 remove following line, keep it for the time being
    event.removePrimaryKey();
    Object builder = esRequestMapper.map(event);
    if (buffered(builder)) {
      boolean addRes = batchBuffer.add(
          new SyncWrapper<>(event, ((WriteRequestBuilder) builder).request()));
      flushIfReachSizeLimit();
      return addRes;
    } else {
      bulkByScrollRequest(event, ((AbstractBulkByScrollRequestBuilder) builder), 0);
    }
    return true;
  }

  private boolean buffered(Object builder) {
    return builder instanceof WriteRequestBuilder;
  }

  private void bulkByScrollRequest(SyncData data, AbstractBulkByScrollRequestBuilder builder,
      int count) {
    builder.execute(new ActionListener<BulkByScrollResponse>() {
      @Override
      public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
        MDC.put(IdGenerator.EID, data.getEventId());
        if (bulkByScrollResponse.getUpdated() == 0
            && bulkByScrollResponse.getDeleted() == 0) {
          logger.warn("Fail to {} by query {}: no documents changed",
              builder.request(), builder.source());
          waitRefresh();
          retry(new IllegalStateException("Fail to update/delete"));
        } else {
          ack.remove(data.getSourceIdentifier(), data.getDataId());
        }
      }

      @Override
      public void onFailure(Exception e) {
        MDC.put(IdGenerator.EID, data.getEventId());
        logger.error("Fail to {} by query: {}", builder.request(), builder.source(), e);
        retry(e);
      }

      private void retry(Exception e) {
        if (count + 1 >= batch.getMaxRetry()) {
          singleRequest.log(data, e.getMessage());
          ack.remove(data.getSourceIdentifier(), data.getDataId());
          return;
        }
        bulkByScrollRequest(data, builder, count + 1);
      }
    });
  }

  private void waitRefresh() {
    if (refreshInterval == 0) return;
    try {
      Thread.sleep(refreshInterval);
    } catch (InterruptedException ignored) { }
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
    List<SyncWrapper<WriteRequest>> aim = batchBuffer.flush();
    buildAndSend(aim);
  }

  @Override
  public void ackSuccess(List<SyncWrapper<WriteRequest>> aim) {
    for (SyncWrapper wrapper : aim) {
      ack.remove(wrapper.getSourceId(), wrapper.getSyncDataId());
    }
  }

  @Override
  public void retryFailed(List<SyncWrapper<WriteRequest>> aim, Exception e) {
    Map<String, String> failedDocuments = ((ElasticsearchBulkException) e).getFailedDocuments();
    for (SyncWrapper<WriteRequest> wrapper : aim) {
      WriteRequest request = wrapper.getData();
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
        if (retriable(e)) {
          logger.info("Retry request: {}", reqStr == null ? request : reqStr);
          if (wrapper.retryCount() < batch.getMaxRetry()) {
            batchBuffer.addFirst(wrapper);
          } else {
            logger.error("Max retry exceed, write {} to fail.log: {}", wrapper, failedDocuments.get(id));
            singleRequest.log(wrapper.getEvent(), failedDocuments.get(id));
            ack.remove(wrapper.getSourceId(), wrapper.getSyncDataId());
          }
        } else {
          logger.error("Met non-retriable error, write {} to fail.log: {}", wrapper, failedDocuments.get(id));
          singleRequest.log(wrapper.getEvent(), failedDocuments.get(id));
          ack.remove(wrapper.getSourceId(), wrapper.getSyncDataId());
        }
      } else {
        ack.remove(wrapper.getSourceId(), wrapper.getSyncDataId());
      }
    }
  }

  @Override
  public boolean retriable(Exception e) {
    return true;
  }

  @ThreadSafe(safe = {TransportClient.class, BatchBuffer.class})
  @Override
  public void flushIfReachSizeLimit() {
    @SuppressWarnings("unchecked")
    List<SyncWrapper<WriteRequest>> aim = batchBuffer.flushIfReachSizeLimit();
    buildAndSend(aim);
  }

  private void buildAndSend(List<SyncWrapper<WriteRequest>> aim) {
    if (aim != null && aim.size() != 0) {
      try {
        buildRequest(aim);
        ackSuccess(aim);
      } catch (ElasticsearchBulkException e) {
        retryFailed(aim, e);
      }
    }
  }

  private void buildRequest(List<SyncWrapper<WriteRequest>> aim) {
    StringJoiner joiner = new StringJoiner(",", "[", "]");
    // BulkProcessor?
    BulkRequestBuilder bulkRequest = client.prepareBulk();
    for (SyncWrapper<WriteRequest> requestWrapper : aim) {
      WriteRequest request = requestWrapper.getData();
      if (request instanceof IndexRequest) {
        joiner.add(request.toString());
        bulkRequest.add((IndexRequest) request);
      } else if (request instanceof UpdateRequest) {
        joiner.add(toString(((UpdateRequest) request)));
        bulkRequest.add(((UpdateRequest) request));
      } else if (request instanceof DeleteRequest) {
        joiner.add(request.toString());
        bulkRequest.add(((DeleteRequest) request));
      }
    }
    logger.info("Sending to Elasticsearch: {}", joiner);
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
