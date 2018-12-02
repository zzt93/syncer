package com.github.zzt93.syncer.health.consumer;

import com.github.zzt93.syncer.config.consumer.common.Connection;
import com.github.zzt93.syncer.health.Health;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerHealth {

  private final String id;
  /**
   * key is outputs channel's connection string
   * @see Connection#connectionIdentifier()
   */
  private final ConcurrentHashMap<String, OutputHealth> outputs = new ConcurrentHashMap<>();
  private Health filterStatus;

  public ConsumerHealth(String id, Health filterStatus) {
    this.id = id;
    this.filterStatus = filterStatus;
  }

  public ConsumerHealth(String id, String output, Health status) {
    this.id = id;
    this.filterStatus = Health.green();
    outputs.put(output, new OutputHealth(output, status));
  }

  public Health getFilterStatus() {
    return filterStatus;
  }

  public Health getStatus() {
    Health res = filterStatus;
    for (OutputHealth value : outputs.values()) {
      res = res.and(value.getStatus());
    }
    return res;
  }

  public String getId() {
    return id;
  }

  public Map<String, OutputHealth> getOutputs() {
    return outputs;
  }

  public ConsumerHealth filter(Health status) {
    filterStatus = status;
    return this;
  }

  public ConsumerHealth output(String output, Health status) {
    outputs.compute(output, (k, v) -> {
      if (v == null) {
        return new OutputHealth(output, status);
      }
      return v.status(status);
    });
    return this;
  }

  public Health getHealth() {
    Health health = filterStatus;
    for (OutputHealth outputHealth : outputs.values()) {
      health = health.and(outputHealth.getStatus());
    }
    return health;
  }
}
