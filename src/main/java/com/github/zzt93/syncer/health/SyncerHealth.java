package com.github.zzt93.syncer.health;

import com.github.zzt93.syncer.health.consumer.ConsumerHealth;
import com.github.zzt93.syncer.health.producer.ProducerHealth;

import java.util.concurrent.ConcurrentHashMap;

public class SyncerHealth {

  private static ConcurrentHashMap<String, ConsumerHealth> consumers = new ConcurrentHashMap<>();
  private static ConcurrentHashMap<String, ProducerHealth> producers = new ConcurrentHashMap<>();

  public static void init() {

  }

  public static void consumer(String id, String output, Health status) {

  }

  public static void producer(String connection, Health status) {

  }
}
