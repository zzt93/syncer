package com.github.zzt93.syncer.health;

import com.github.zzt93.syncer.Starter;
import com.github.zzt93.syncer.config.pipeline.PipelineConfig;
import com.github.zzt93.syncer.config.pipeline.common.Connection;
import com.github.zzt93.syncer.health.consumer.ConsumerHealth;
import com.github.zzt93.syncer.health.producer.ProducerHealth;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class SyncerHealth {

  /**
   * key is producer's connection id
   * @see Connection#connectionIdentifier()
   */
  private static ConcurrentHashMap<String, ProducerHealth> producers = new ConcurrentHashMap<>();
  /**
   * key is consumer's id
   * @see PipelineConfig#getConsumerId()
   */
  private static ConcurrentHashMap<String, ConsumerHealth> consumers = new ConcurrentHashMap<>();

  /**
   * init system's health as GREEN
   * @see com.github.zzt93.syncer.health.Health.HealthStatus#GREEN
   */
  public static void init(List<Starter> starters) {
    for (Starter starter : starters) {
      starter.registerToHealthCenter();
    }
  }

  public static void consumer(String id, Health status) {
    consumers.compute(id, (k, v) -> {
      if (v == null) {
        return new ConsumerHealth(id, status);
      }
      return v.and(status);
    });
  }

  public static void consumer(String id, String output, Health status) {
    consumers.compute(id, (k, v) -> {
      if (v == null) {
        return new ConsumerHealth(id, output, status);
      }
      return v.and(output, status);
    });
  }

  public static void producer(String connection, Health status) {
    producers.compute(connection, (k, v) -> {
      if (v == null) {
        return new ProducerHealth(connection, status);
      }
      return v.status(status);
    });
  }

  public static String toJson() {
    return null;
  }

}
