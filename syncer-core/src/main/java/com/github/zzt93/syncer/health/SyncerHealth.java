package com.github.zzt93.syncer.health;

import com.github.zzt93.syncer.Starter;
import com.github.zzt93.syncer.config.common.Connection;
import com.github.zzt93.syncer.config.consumer.ConsumerConfig;
import com.github.zzt93.syncer.health.consumer.ConsumerHealth;
import com.github.zzt93.syncer.health.producer.ProducerHealth;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class SyncerHealth {

  private static final Gson pretty = new GsonBuilder().setPrettyPrinting().create();
  /**
   * key is producer's connection id
   * @see Connection#connectionIdentifier()
   */
  private static ConcurrentHashMap<String, ProducerHealth> producers = new ConcurrentHashMap<>();
  /**
   * key is consumer's id
   * @see ConsumerConfig#getConsumerId()
   */
  private static ConcurrentHashMap<String, ConsumerHealth> consumers = new ConcurrentHashMap<>();

  /**
   * init system's health as GREEN
   * @see Health.HealthStatus#GREEN
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
      return v.filter(status);
    });
  }

  public static void consumer(String id, String output, Health status) {
    consumers.compute(id, (k, v) -> {
      if (v == null) {
        return new ConsumerHealth(id, output, status);
      }
      return v.output(output, status);
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
    Health overall = Health.green();
    ArrayList<ProducerHealth> producer = new ArrayList<>(producers.values());
    for (ProducerHealth p : producer) {
      overall = overall.and(p.getStatus());
    }
    ArrayList<ConsumerHealth> consumer = new ArrayList<>(consumers.values());
    for (ConsumerHealth c : consumer) {
      overall = overall.and(c.getHealth());
    }
    HashMap<String, Object> obj = new HashMap<>();
    obj.put("overall", overall);
    obj.put("producer", producer);
    obj.put("consumer", consumer);
    return pretty.toJson(obj);
  }

}
