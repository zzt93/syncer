package com.github.zzt93.syncer.consumer.output.channel.elastic;

import com.github.zzt93.syncer.common.LogbackLoggingField;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.github.zzt93.syncer.common.util.FallBackPolicy;
import com.github.zzt93.syncer.config.common.ElasticsearchConnection;
import com.github.zzt93.syncer.config.consumer.output.FailureLogConfig;
import com.github.zzt93.syncer.config.consumer.output.PipelineBatchConfig;
import com.github.zzt93.syncer.config.consumer.output.elastic.Elasticsearch;
import com.github.zzt93.syncer.config.syncer.SyncerOutputMeta;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.batch.BatchBuffer;
import com.github.zzt93.syncer.consumer.output.channel.BufferedChannel;
import com.github.zzt93.syncer.consumer.output.channel.SyncWrapper;
import com.github.zzt93.syncer.consumer.output.failure.FailureEntry;
import com.github.zzt93.syncer.consumer.output.failure.FailureLog;
import com.github.zzt93.syncer.health.Health;
import com.github.zzt93.syncer.health.SyncerHealth;
import com.google.gson.reflect.TypeToken;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.reindex.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * @author zzt
 */
public class ElasticsearchChannel implements BufferedChannel<DocWriteRequest> {

  private final BatchBuffer<SyncWrapper<DocWriteRequest>> batchBuffer;
  private final Ack ack;
  // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-low-usage-maven.html
  // TODO 18/7/12 change to rest client:
  private final RestHighLevelClient client;
  private final FailureLog<SyncData> singleRequest;
  private final ESRequestMapper esRequestMapper;

  private final Logger logger = LoggerFactory.getLogger(ElasticsearchChannel.class);
  private final PipelineBatchConfig batchConfig;
  private final long refreshInterval;
  private final String id;
  private final String consumerId;
  private final AtomicBoolean closed = new AtomicBoolean(false);


  public ElasticsearchChannel(Elasticsearch elasticsearch, SyncerOutputMeta outputMeta, Ack ack)
      throws Exception {
    ElasticsearchConnection connection = elasticsearch.getConnection();
    id = connection.connectionIdentifier();
    consumerId = elasticsearch.getConsumerId();
    client = connection.esClient();
    refreshInterval = elasticsearch.getRefreshInMillis();
    this.batchBuffer = new BatchBuffer<>(elasticsearch.getBatch());
    this.batchConfig = elasticsearch.getBatch();
    this.esRequestMapper = new ESRequestMapper(client, elasticsearch.getRequestMapping());
    this.ack = ack;
    FailureLogConfig failureLog = elasticsearch.getFailureLog();
    Path path = Paths.get(outputMeta.getFailureLogDir(), id);
    singleRequest = FailureLog.getLogger(path, failureLog, new TypeToken<FailureEntry<SyncData>>() {
    });
  }


  @ThreadSafe(safe = {ESRequestMapper.class, BatchBuffer.class})
  @Override
  public boolean output(SyncData event) throws InterruptedException {
    if (closed.get()) {
      return false;
    }
    if (event.removePrimaryKey()) {
      logger.warn("Include primary key in `_source` is usually not necessary, remove it");
    }
    Object request = esRequestMapper.map(event);
    if (buffered(request)) {
      boolean addRes = batchBuffer.add(new SyncWrapper<>(event, ((DocWriteRequest) request)));
      BufferedChannel.super.flushAndSetFlushDone(true);
      return addRes;
    } else {
      return sleepInConnectionLost((sleepInSecond) -> {
        bulkByScrollRequest(event, ((AbstractBulkByScrollRequest) request), 0);
        return true;
      });
    }
  }

  private boolean buffered(Object builder) {
    return builder instanceof DocWriteRequest;
  }

  private void bulkByScrollRequest(SyncData data, AbstractBulkByScrollRequest bulkByScrollRequest,
                                   int count) {
    if (closed.get()) {
      return;
    }

    ActionListener<BulkByScrollResponse> actionListener = new ActionListener<BulkByScrollResponse>() {
      @Override
      public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
        MDC.put(LogbackLoggingField.EID, data.getEventId());
        if (bulkByScrollResponse.getUpdated() == 0
            && bulkByScrollResponse.getDeleted() == 0) {
          if (count == 0) {// only log at first failure
            logger.warn("No documents changed of {}:\n {}", bulkByScrollRequest.getSearchRequest(), bulkByScrollRequest.getDescription());
          }
          try {
            waitRefresh();
          } catch (InterruptedException e) {
            logger.warn("Interrupt thread {}", Thread.currentThread().getName());
            return;
          }
          retry(null, false);
        } else {
          if (count > 0) {
            logger.warn("Finally succeed to {}:\n {}", bulkByScrollRequest.getSearchRequest(), bulkByScrollRequest.getDescription());
          }
          ack.remove(data.getSourceIdentifier(), data.getDataId());
        }
      }

      @Override
      public void onFailure(Exception e) {
        MDC.put(LogbackLoggingField.EID, data.getEventId());
        logger.error("Fail to {}:\n {}", bulkByScrollRequest.getSearchRequest(), bulkByScrollRequest.getDescription(), e);
        retry(e, true);
      }

      private void retry(Exception e, boolean log) {
        if (count + 1 >= batchConfig.getMaxRetry()) {
          if (log) {
            singleRequest.log(data, e.getMessage());
          } else {
            logger.warn("No documents updated/deleted by query: {}", data);
          }
          ack.remove(data.getSourceIdentifier(), data.getDataId());
          return;
        }
        bulkByScrollRequest(data, bulkByScrollRequest, count + 1);
      }
    };

    if (bulkByScrollRequest instanceof DeleteByQueryRequest) {
      client.deleteByQueryAsync((DeleteByQueryRequest) bulkByScrollRequest, RequestOptions.DEFAULT, actionListener);
    } else if (bulkByScrollRequest instanceof UpdateByQueryRequest) {
      client.updateByQueryAsync((UpdateByQueryRequest) bulkByScrollRequest, RequestOptions.DEFAULT, actionListener);
    }

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
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    BufferedChannel.super.close();

    try {
      client.close();
    } catch (IOException e) {
      logger.error("Fail to close ES channel", e);
    }
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public long getDelay() {
    return batchConfig.getDelay();
  }

  @Override
  public TimeUnit getDelayUnit() {
    return batchConfig.getDelayTimeUnit();
  }

  @ThreadSafe(safe = {RestHighLevelClient.class, BatchBuffer.class})
  @Override
  public boolean flush() throws InterruptedException {
    List<SyncWrapper<DocWriteRequest>> aim = batchBuffer.flush();
    buildSendProcess(aim);
    return aim != null;
  }

  @Override
  public void ackSuccess(List<SyncWrapper<DocWriteRequest>> aim) {
    for (SyncWrapper wrapper : aim) {
      ack.remove(wrapper.getSourceId(), wrapper.getSyncDataId());
    }
  }

  @Override
  public void retryFailed(List<SyncWrapper<DocWriteRequest>> aim, Throwable e) {
    BulkItemResponse[] items = ((ElasticsearchBulkException) e).getBulkItemResponses().getItems();
    LinkedList<SyncWrapper<DocWriteRequest>> tmp = new LinkedList<>();
    for (int i = 0; i < aim.size(); i++) {
      BulkItemResponse item = items[i];
      SyncWrapper<DocWriteRequest> wrapper = aim.get(i);
      if (!item.isFailed()) {
        ack.remove(wrapper.getSourceId(), wrapper.getSyncDataId());
        continue;
      }

      // failed item handle
      ErrorLevel level = level(e, wrapper, batchConfig.getMaxRetry());
      if (level.retriable()) {
        logger.info("Retry {} because {}", wrapper.getData(), item.getFailure());
        handle404(wrapper, item);
        tmp.add(wrapper);
      } else {
        logger.error("Met {} in {} because {}", level, wrapper, item.getFailure());
        switch (level) {
          case MAX_TRY_EXCEED:
          case SYNCER_BUG:
          case WARN:
            singleRequest.log(wrapper.getEvent(), item.getFailure().toString());
            ack.remove(wrapper.getSourceId(), wrapper.getSyncDataId());
            break;
        }

      }
    }
    batchBuffer.addAllInHead(tmp);
  }

  @Override
  public ErrorLevel level(Throwable e, SyncWrapper wrapper, int maxTry) {
    if (e instanceof IndexNotFoundException) {
      return ErrorLevel.WARN;
    }
    return BufferedChannel.super.level(e, wrapper, maxTry);
  }

  private void handle404(SyncWrapper<DocWriteRequest> wrapper, BulkItemResponse item) {
    if (item.getFailure().getCause() instanceof DocumentMissingException) {
      logger.warn("Make update request upsert to resolve DocumentMissingException");
      UpdateRequest request = ((UpdateRequest) wrapper.getData());
      if (request.doc() != null) {
        request.docAsUpsert(true);
      } else {
        request.upsert(ESRequestMapper.getUpsert(wrapper.getEvent())).scriptedUpsert(true);
      }
    }
  }

  @Override
  public boolean checkpoint() {
    return ack.flush();
  }

  @ThreadSafe(safe = {RestHighLevelClient.class, BatchBuffer.class})
  @Override
  public boolean flushIfReachSizeLimit() throws InterruptedException {
    List<SyncWrapper<DocWriteRequest>> aim = batchBuffer.flushIfReachSizeLimit();
    buildSendProcess(aim);
    return aim != null;
  }

  @Override
  public void setFlushDone() {
    batchBuffer.flushDone();
  }

  private void buildSendProcess(List<SyncWrapper<DocWriteRequest>> aim) throws InterruptedException {
    if (aim != null) {
      logger.info("Flush batch({})", aim.size());
      BulkResponse bulkResponse = buildAndSend(aim);
      if (!bulkResponse.hasFailures()) {
        ackSuccess(aim);
      } else {
        retryFailed(aim, new ElasticsearchBulkException("Bulk request has failures", bulkResponse));
      }
    }
  }

  /**
   * Sending to ES in synchronous way to avoid disorder between events: e.g.
   * <pre>
   *   sending: [update 1], [insert 2]
   *   sending: [delete 1] --> may arrive ES first
   * </pre>
   *
   * @throws InterruptedException throw when shutdown
   */
  private BulkResponse buildAndSend(List<SyncWrapper<DocWriteRequest>> aim)
      throws InterruptedException {
    // BulkProcessor?
    BulkRequest bulkRequest = new BulkRequest();
    if (logger.isDebugEnabled()) {
      StringJoiner joiner = new StringJoiner(",", "[", "]");
      for (SyncWrapper<DocWriteRequest> requestWrapper : aim) {
        DocWriteRequest request = requestWrapper.getData();
        joiner.add(request.toString());
        bulkRequest.add(request);
      }
      // This buffer is shared by all filter thread,
      // so events scheduled to different queue & filter thread will
      // all in this buffer (Of course, will not affect the order of related events)
      logger.debug("Sending {}", joiner);
    } else {
      for (SyncWrapper<DocWriteRequest> requestWrapper : aim) {
        DocWriteRequest request = requestWrapper.getData();
        bulkRequest.add(request);
      }
    }

    return sleepInConnectionLost((sleepInSecond) -> {
      try {
        return client.bulk(bulkRequest, RequestOptions.DEFAULT);
      } catch (IOException e) {
        logger.error("", e);
        SyncerHealth.consumer(consumerId, id, Health.red(e.getMessage()));
        return null;
      }
    });
  }

  /**
   * @param supplier return null if it fails to connect to ES
   */
  private <R> R sleepInConnectionLost(Function<Long, R> supplier) throws InterruptedException {
    long sleepInSecond = 1;
    while (true) {
      R apply = null;
      try {
        apply = supplier.apply(sleepInSecond);
      } catch (NoNodeAvailableException e) {
        String error = "Fail to connect to ES server, will retry in {}s";
        logger.error(error, sleepInSecond, e);
        SyncerHealth.consumer(consumerId, id, Health.red(error));
      }
      if (apply != null) {
        return apply;
      }
      sleepInSecond = FallBackPolicy.POW_2.next(sleepInSecond, TimeUnit.SECONDS);
      TimeUnit.SECONDS.sleep(sleepInSecond);
    }
  }

}
