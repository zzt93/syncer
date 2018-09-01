package com.github.zzt93.syncer.health;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class Health {

  private final HealthStatus status;
  private final List<String> cause;

  private Health(HealthStatus status, List<String> cause) {
    this.status = status;
    this.cause = cause;
  }

  public static Health green() {
    return new Health(HealthStatus.GREEN, null);
  }

  public static Health yellow(String msg) {
    LinkedList<String> cause = Lists.newLinkedList();
    cause.add(msg);
    return new Health(HealthStatus.YELLOW, cause);
  }

  public static Health red(String msg) {
    LinkedList<String> cause = Lists.newLinkedList();
    cause.add(msg);
    return new Health(HealthStatus.RED, cause);
  }

  public HealthStatus getStatus() {
    return status;
  }

  public List<String> getCause() {
    return cause;
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

}
