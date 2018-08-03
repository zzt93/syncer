package com.github.zzt93.syncer.consumer.output.channel.elastic;

import com.github.zzt93.syncer.common.IdGenerator;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncWrapper;
import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.github.zzt93.syncer.common.util.FallBackPolicy;
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
import java.util.List;
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
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
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

  private final BatchBuffer<SyncWrapper<WriteRequest>> batchBuffer;
  private final ESRequestMapper esRequestMapper;
  private final Logger logger = LoggerFactory.getLogger(ElasticsearchChannel.class);
  // TODO 18/7/12 change to rest client:
  // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-low-usage-maven.html
  private final AbstractClient client;
  private final PipelineBatch batch;
  private final Ack ack;
  private final FailureLog<SyncData> singleRequest;
  private long refreshInterval = 1000;

  public ElasticsearchChannel(Elasticsearch elasticsearch, SyncerOutputMeta outputMeta, Ack ack)
      throws Exception {
    ElasticsearchConnection connection = elasticsearch.getConnection();
    client = connection.esClient();
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

  static String toString(UpdateRequest request) {
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
  public boolean output(SyncData event) throws InterruptedException {
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
          if (count == 0) {// only log at first failure
            logger.warn("No documents changed of {}: {}", builder.request(), builder.source());
          }
          try {
            waitRefresh();
          } catch (InterruptedException e) {
            logger.error("Thread interrupted", e);
            return;
          }
          retry(null, false);
        } else {
          if (count > 0) {
            logger.warn("Finally succeed to {}: {}", builder.request(), builder.source());
          }
          ack.remove(data.getSourceIdentifier(), data.getDataId());
        }
      }

      @Override
      public void onFailure(Exception e) {
        MDC.put(IdGenerator.EID, data.getEventId());
        if (count == 0) {// only log at first failure
          logger.error("Fail to {}: {}", builder.request(), builder.source(), e);
        }
        retry(e, true);
      }

      private void retry(Exception e, boolean log) {
        if (count + 1 >= batch.getMaxRetry()) {
          if (log) {
            singleRequest.log(data, e.getMessage());
          }
          ack.remove(data.getSourceIdentifier(), data.getDataId());
          return;
        }
        bulkByScrollRequest(data, builder, count + 1);
      }
    });
  }

  private void waitRefresh() throws InterruptedException {
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-refresh.html
    // 1. shouldn't force refresh; 2. `wait_for` & `false` not solve the problem
    // wait in case of the document need to update by query is just indexed,
    // and not refreshed, so not visible for user
    if (refreshInterval == 0) {
      return;
    }
    Thread.sleep(refreshInterval);
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
  public void flush() throws InterruptedException {
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
    BulkItemResponse[] items = ((ElasticsearchBulkException) e).getBulkItemResponses().getItems();
    for (int i = 0; i < aim.size(); i++) {
      SyncWrapper<WriteRequest> wrapper = aim.get(i);
      WriteRequest request = wrapper.getData();
      String reqStr = null;
      if (request instanceof UpdateRequest) {
        reqStr = toString((UpdateRequest) request);
      }
      BulkItemResponse item = items[i];
      if (item.isFailed()) {
        if (retriable(e)) {
          logger.info("Retry request: {}", reqStr == null ? request : reqStr);
          if (wrapper.retryCount() < batch.getMaxRetry()) {
            batchBuffer.addFirst(wrapper);
          } else {
            logger.error("Max retry exceed, write {} to fail.log: {}", wrapper, item.getFailure());
            singleRequest.log(wrapper.getEvent(), item.getFailure().toString());
            ack.remove(wrapper.getSourceId(), wrapper.getSyncDataId());
          }
        } else {
          logger.error("Met non-retriable error, write {} to fail.log: {}", wrapper,
              item.getFailure());
          singleRequest.log(wrapper.getEvent(), item.getFailure().toString());
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
  public void flushIfReachSizeLimit() throws InterruptedException {
    @SuppressWarnings("unchecked")
    List<SyncWrapper<WriteRequest>> aim = batchBuffer.flushIfReachSizeLimit();
    buildAndSend(aim);
  }

  private void buildAndSend(List<SyncWrapper<WriteRequest>> aim) throws InterruptedException {
    if (aim != null && aim.size() != 0) {
      BulkResponse bulkResponse = buildRequest(aim);
      if (!bulkResponse.hasFailures()) {
        ackSuccess(aim);
      } else {
        retryFailed(aim, new ElasticsearchBulkException("Bulk request has failures", bulkResponse));
      }
    }
  }

  private BulkResponse buildRequest(List<SyncWrapper<WriteRequest>> aim)
      throws InterruptedException {
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
    while (true) {
      long sleepInSecond = 1;
      try {
        return bulkRequest.execute().actionGet();
      } catch (NoNodeAvailableException e) {
        logger.error("Fail to connect to ES server, will retry in {}s", sleepInSecond, e);
      }
      sleepInSecond = FallBackPolicy.POW_2.next(sleepInSecond, TimeUnit.SECONDS);
      TimeUnit.SECONDS.sleep(sleepInSecond);
    }
  }

}
