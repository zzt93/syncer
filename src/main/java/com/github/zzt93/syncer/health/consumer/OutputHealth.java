package com.github.zzt93.syncer.health.consumer;

import com.github.zzt93.syncer.health.Health;

public class OutputHealth {

  private final String connection;
  private final Health status;

  public OutputHealth(String connection, Health status) {
    this.connection = connection;
    this.status = status;
  }

  public String getConnection() {
    return connection;
  }

  public Health getStatus() {
    return status;
  }

  public OutputHealth status(Health status) {
    return new OutputHealth(connection, status);
  }
}
