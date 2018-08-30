package com.github.zzt93.syncer.health.producer;

import com.github.zzt93.syncer.health.Health;

public class ProducerHealth {

  private final String connection;
  private final Health status;


  public ProducerHealth(String connection, Health status) {
    this.connection = connection;
    this.status = status;
  }

  public String getConnection() {
    return connection;
  }

  public Health getStatus() {
    return status;
  }
}
