package com.github.zzt93.syncer.health;

import java.util.ArrayList;
import java.util.List;

public class Health {

  private final HealthStatus status;
  private final List<String> cause;

  public Health(HealthStatus status, List<String> cause) {
    this.status = status;
    this.cause = cause;
  }

  public HealthStatus getStatus() {
    return status;
  }

  public List<String> getCause() {
    return cause;
  }

  public enum HealthStatus {


    GREEN,
    YELLOW,
    RED,
    ;


    public HealthStatus and(HealthStatus status) {
      if (status.ordinal() > ordinal()) {
        return status;
      }
      return this;
    }
  }

  public Health and(Health health) {
    return new Health(status.and(health.getStatus()), merge(cause, health.cause));
  }

  private List<String> merge(List<String> f, List<String> s) {
    if (f == null) {
      return s;
    }
    if (s == null) {
      return f;
    }
    ArrayList<String> cause = new ArrayList<>(f);
    cause.addAll(s);
    return cause;
  }

}
