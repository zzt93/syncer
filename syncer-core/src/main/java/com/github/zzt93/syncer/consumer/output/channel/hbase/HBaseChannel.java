package com.github.zzt93.syncer.consumer.output.channel.hbase;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.util.NamedThreadFactory;
import com.github.zzt93.syncer.config.common.HBaseConnection;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.consumer.output.PipelineBatchConfig;
import com.github.zzt93.syncer.config.consumer.output.habse.HBase;
import com.github.zzt93.syncer.config.syncer.SyncerOutputMeta;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.AckChannel;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;
import com.github.zzt93.syncer.consumer.output.channel.SyncWrapper;
import com.github.zzt93.syncer.consumer.output.failure.FailureEntry;
import com.github.zzt93.syncer.consumer.output.failure.FailureLog;
import com.google.gson.reflect.TypeToken;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Data
public class HBaseChannel implements OutputChannel, AckChannel<Mutation> {
  private final AsyncConnection connection;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final PipelineBatchConfig batchConfig;
  private final Ack ack;
  private final HBaseMapper hBaseMapper;
  private final ExecutorService hbaseService;
  private final String outputId;
  private final FailureLog<SyncData> failureLog;
  private final ConcurrentHashMap<TableName, AsyncBufferedMutator> mutations = new ConcurrentHashMap<>();


  public HBaseChannel(HBase hBase, SyncerOutputMeta outputMeta, Ack ack) {
    this.batchConfig = hBase.getBatch();
    this.ack = ack;
    hBaseMapper = new HBaseMapper();

    HBaseConnection connection = hBase.getConnection();
    try {
      CompletableFuture<AsyncConnection> asyncConnection = ConnectionFactory.createAsyncConnection(connection.conf());
      this.connection = asyncConnection.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new InvalidConfigException(e);
    }
    outputId = connection.connectionIdentifier();
    hbaseService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(10),
        new NamedThreadFactory("syncer-" + outputId + "-output-hbase"),
        // discard flush job if too much
        new ThreadPoolExecutor.DiscardPolicy());
    failureLog = FailureLog.getLogger(Paths.get(outputMeta.getFailureLogDir(), connection.connectionIdentifier()),
        hBase.getFailureLog(), new TypeToken<FailureEntry<SyncWrapper<String>>>() {
        });
  }

  @Override
  public boolean output(SyncData event) throws InterruptedException {
    if (closed.get()) {
      return false;
    }

    Mutation mutation = hBaseMapper.map(event);
    log.debug("Add {}", mutation);

    hbaseService.submit(() -> {
      try {
        doSend(new SyncWrapper<>(event, mutation));
      } catch (Throwable e) {
        log.error("", e);
      }
    });

    return true;
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    for (AsyncBufferedMutator mutator : mutations.values()) {
      mutator.close();
    }
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (IOException e) {
      log.error("Fail to close {}", connection);
    }
  }

  @Override
  public String id() {
    return outputId;
  }

  public void doSend(SyncWrapper<Mutation> wrapper) {
    TableName tableName = TableName.valueOf(wrapper.getEvent().getHBaseTable());
    CompletableFuture<Void> future = mutations.computeIfAbsent(tableName, t -> connection.getBufferedMutatorBuilder(t).setMaxRetries(batchConfig.getMaxRetry()).setWriteBufferSize(batchConfig.getSize()).setWriteBufferPeriodicFlush(batchConfig.getDelay(), batchConfig.getDelayTimeUnit()).build()).mutate(wrapper.getData());
    future.whenComplete((v, t) -> {
      if (t != null) {
        retryFailed(Collections.singletonList(wrapper), t);
      } else {
        ackSuccess(Collections.singletonList(wrapper));
      }
    });
  }

  @Override
  public void retryFailed(List<SyncWrapper<Mutation>> aim, Throwable e) {
    RetriesExhaustedException item = ((RetriesExhaustedException) e);
    for (SyncWrapper<Mutation> wrapper : aim) {
      ErrorLevel level = level(e, wrapper, batchConfig.getMaxRetry());
      if (level.retriable()) {
        log.info("Retry {} because {}", wrapper, item.getMessage());
        doSend(wrapper);
      } else {
        log.error("Met {} in {} because {}", level, wrapper, item.getMessage());
        switch (level) {
          case MAX_TRY_EXCEED:
          case SYNCER_BUG:
          case WARN:
            failureLog.log(wrapper.getEvent(), item.getMessage().toString());
            ack.remove(wrapper.getSourceId(), wrapper.getSyncDataId());
            break;
        }
      }
    }
  }

  @Override
  public ErrorLevel level(Throwable e, SyncWrapper wrapper, int maxTry) {
    return ErrorLevel.MAX_TRY_EXCEED;
  }
}
