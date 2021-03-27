package com.github.zzt93.syncer.consumer.output.channel.elastic;

import com.github.zzt93.syncer.common.LogbackLoggingField;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.thread.EventLoop;
import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.github.zzt93.syncer.common.util.FallBackPolicy;
import com.github.zzt93.syncer.common.util.NamedThreadFactory;
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
import lombok.Getter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
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
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequestBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * @author zzt
 */
@Getter
public class ElasticsearchChannel implements BufferedChannel<WriteRequest> {

  private final BatchBuffer<SyncWrapper<WriteRequest>> batchBuffer;
  private final Ack ack;
  // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-low-usage-maven.html
  // TODO 18/7/12 change to rest client:
  private final AbstractClient client;
  private final FailureLog<SyncData> singleRequest;
  private final ESRequestMapper esRequestMapper;

  private final Logger logger = LoggerFactory.getLogger(ElasticsearchChannel.class);
  private final PipelineBatchConfig batchConfig;
  private final long refreshInterval;
  private final String id;
  private final String consumerId;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final BlockingQueue<SyncData>[] queues;
  private final int worker;
  private final ExecutorService esService;


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
    worker = outputMeta.getWorker();
    queues = new ArrayBlockingQueue[worker];
    esService = Executors
        .newFixedThreadPool(worker, new NamedThreadFactory("syncer-" + id + "-output-es"));

    for (int i = 0; i < queues.length; i++) {
      queues[i] = new ArrayBlockingQueue<>(outputMeta.getCapacity());
    }
    singleRequest = FailureLog.getLogger(path, failureLog, new TypeToken<FailureEntry<SyncData>>() {
    });
  }

  public void start() {
    for (int i = 0; i < worker; i++) {
      esService.submit(new EsOutputJob(queues[i], this));
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
    if (closed.get()) {
      return false;
    }
    if (event.removePrimaryKey()) {
      logger.info("Include primary key in `_source` is usually not necessary, remove it");
    }

    queues[(int) (event.getPartitionKey()%worker)].put(event);
    return true;
  }

  public static class EsOutputJob implements EventLoop {
    private final BlockingQueue<SyncData> queue;
    private final ElasticsearchChannel elasticsearchChannel;

    public EsOutputJob(BlockingQueue<SyncData> queue, ElasticsearchChannel elasticsearchChannel) {
      this.queue = queue;
      this.elasticsearchChannel = elasticsearchChannel;
    }

    @Override
    public void loop() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          elasticsearchChannel.mapAndFlush(queue);
        } catch (InterruptedException e) {
          logger.warn("[Shutting down] Filter job interrupted");
          return;
        }
      }
    }
  }

  public void mapAndFlush(BlockingQueue<SyncData> queue) throws InterruptedException {
    SyncData event = queue.take();
    logger.debug("{}", event);
    Object builder = esRequestMapper.map(event);
    logger.debug("{}", event);

    if (buffered(builder)) {
      batchBuffer.add(
          new SyncWrapper<>(event, ((WriteRequestBuilder) builder).request()));
      BufferedChannel.super.flushAndSetFlushDone(true);
    } else {
      sleepInConnectionLost((sleepInSecond) -> {
        bulkByScrollRequest(event, ((AbstractBulkByScrollRequestBuilder) builder), 0);
        return true;
      });
    }
  }

  private boolean buffered(Object builder) {
    return builder instanceof WriteRequestBuilder;
  }

  private void bulkByScrollRequest(SyncData data, AbstractBulkByScrollRequestBuilder builder,
      int count) {
    if (closed.get()) {
      return;
    }
    builder.execute(new ActionListener<BulkByScrollResponse>() {
      @Override
      public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
        MDC.put(LogbackLoggingField.EID, data.getEventId());
        if (bulkByScrollResponse.getUpdated() == 0
            && bulkByScrollResponse.getDeleted() == 0) {
          if (count == 0) {// only log at first failure
            logger.warn("No documents changed of {}:\n {}", builder.request(), builder.source());
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
            logger.warn("Finally succeed to {}:\n {}", builder.request(), builder.source());
          }
          ack.remove(data.getSourceIdentifier(), data.getDataId());
        }
      }

      @Override
      public void onFailure(Exception e) {
        MDC.put(LogbackLoggingField.EID, data.getEventId());
        logger.error("Fail to {}:\n {}", builder.request(), builder.source(), e);
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
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    BufferedChannel.super.close();

    // client
    client.close();
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public void retryFailed(List<SyncWrapper<WriteRequest>> aim, Throwable e) {
    BulkItemResponse[] items = ((ElasticsearchBulkException) e).getBulkItemResponses().getItems();
    LinkedList<SyncWrapper<WriteRequest>> tmp = new LinkedList<>();
    for (int i = 0; i < aim.size(); i++) {
      BulkItemResponse item = items[i];
      SyncWrapper<WriteRequest> wrapper = aim.get(i);
      if (!item.isFailed()) {
        ack.remove(wrapper.getSourceId(), wrapper.getSyncDataId());
        continue;
      }

      // failed item handle
      ErrorLevel level = level(e, wrapper, batchConfig.getMaxRetry());
      if (level.retriable()) {
        logger.info("Retry {} because {}", requestStr(wrapper), item.getFailure());
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

  private Object requestStr(SyncWrapper<WriteRequest> wrapper) {
    WriteRequest request = wrapper.getData();
    String reqStr = null;
    if (request instanceof UpdateRequest) {
      reqStr = toString((UpdateRequest) request);
    }
    return reqStr == null ? request : reqStr;
  }

  private void handle404(SyncWrapper<WriteRequest> wrapper, BulkItemResponse item) {
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
  public void batchAndRetry(List<SyncWrapper<WriteRequest>> aim) throws InterruptedException {
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
  private BulkResponse buildAndSend(List<SyncWrapper<WriteRequest>> aim)
      throws InterruptedException {
    // BulkProcessor?
    BulkRequestBuilder bulkRequest = client.prepareBulk();
    if (logger.isDebugEnabled()) {
      StringJoiner joiner = new StringJoiner(",", "[", "]");
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
      // This buffer is shared by all filter thread,
      // so events scheduled to different queue & filter thread will
      // all in this buffer (Of course, will not affect the order of related events)
      logger.debug("Sending {}", joiner);
    } else {
      for (SyncWrapper<WriteRequest> requestWrapper : aim) {
        WriteRequest request = requestWrapper.getData();
        if (request instanceof IndexRequest) {
          bulkRequest.add((IndexRequest) request);
        } else if (request instanceof UpdateRequest) {
          bulkRequest.add(((UpdateRequest) request));
        } else if (request instanceof DeleteRequest) {
          bulkRequest.add(((DeleteRequest) request));
        }
      }
    }

    return sleepInConnectionLost((sleepInSecond) -> {
      // TODO 2020/6/9 change to callback
      ListenableActionFuture<BulkResponse> future = bulkRequest.execute();
      try {
        return future.get();
      } catch (ExecutionException e) {
        logger.error("", e);
        SyncerHealth.consumer(consumerId, id, Health.red(e.getMessage()));
        return null;
      } catch (InterruptedException e) {
        logger.info("Future interrupted");
        Thread.currentThread().interrupt();
        return future.actionGet();
      }
    });
  }

  /**
   *
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
