package com.github.zzt93.syncer.health.consumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerHealth {

  private final String id;
  private final Map<String, OutputHealth> output = new ConcurrentHashMap<>();

  public ConsumerHealth(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public Map<String, OutputHealth> getOutput() {
    return output;
  }
}
