package com.github.zzt93.syncer.health;

import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Set;

public class Health {

  private final HealthStatus status;
  private final Set<String> cause;

  private Health(HealthStatus status, Set<String> cause) {
    this.status = status;
    this.cause = cause;
  }

  public static Health green() {
    return new Health(HealthStatus.GREEN, null);
  }

  public static Health inactive(String msg) {
    return new Health(HealthStatus.INACTIVE, Sets.newHashSet(msg));
  }

  public static Health yellow(String msg) {
    return new Health(HealthStatus.YELLOW, Sets.newHashSet(msg));
  }

  public static Health red(String msg) {
    return new Health(HealthStatus.RED, Sets.newHashSet(msg));
  }

  public HealthStatus getStatus() {
    return status;
  }

  public Set<String> getCause() {
    return cause;
  }

  public Health and(Health health) {
    return new Health(status.and(health.getStatus()), merge(cause, health.cause));
  }

  private Set<String> merge(Set<String> f, Set<String> s) {
    if (f == null) {
      return s;
    }
    if (s == null) {
      return f;
    }
    HashSet<String> set = Sets.newHashSet(f);
    set.addAll(s);
    return set;
  }

  public enum HealthStatus {
    // not active state
    INACTIVE,
    // active state
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
