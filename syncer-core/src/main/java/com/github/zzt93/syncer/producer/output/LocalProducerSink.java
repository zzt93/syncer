package com.github.zzt93.syncer.producer.output;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.consumer.ConsumerSource;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author zzt
 */
@Slf4j
public class LocalProducerSink implements ProducerSink {

  private final ArrayBlockingQueue<SyncData> hold = new ArrayBlockingQueue<>(100000);
  private final ConsumerSource consumerSource;

  public LocalProducerSink(ConsumerSource consumerSource) {
    this.consumerSource = consumerSource;
  }

  @Override
  public boolean output(SyncData data) {
    holdForColdStart(data);
    return consumerSource.input(data);
  }

  @Override
  public boolean output(SyncData[] data) {
    holdForColdStart(data);
    return consumerSource.input(data);
  }

  @Override
  public ConsumerSource remote() {
    return consumerSource;
  }

  private static final String EMPTY = "";
  private volatile String coldStartingRepo = EMPTY;
  private volatile String coldStartingEntity = EMPTY;
  @SneakyThrows
  private void holdForColdStart(SyncData... aim) {
    for (SyncData syncData : aim) {
      if (syncData == null) {
        continue;
      }
      if (coldStartingRepo.equals(syncData.getRepo()) && coldStartingEntity.equals(syncData.getEntity())) {
        hold.put(syncData);
      }
    }
  }

  @Override
  public void markColdStart(String repo, String entity) {
    coldStartingRepo = repo;
    coldStartingEntity = entity;
  }

  @Override
  public boolean coldOutput(SyncData[] data) {
    return consumerSource.coldInput(data);
  }

  @Override
  public void markColdStartDoneAndFlush() {
    coldStartingRepo = EMPTY;
    coldStartingEntity = EMPTY;
    if (!hold.isEmpty()) {
      log.debug("{}", hold.size());
      consumerSource.input(hold);
      hold.clear();
    }
  }

  @Override
  public String toString() {
    return "LocalProducerSink{" +
        "consumerSource=" + consumerSource +
        '}';
  }
}
