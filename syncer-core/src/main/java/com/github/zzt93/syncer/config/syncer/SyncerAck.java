package com.github.zzt93.syncer.config.syncer;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.concurrent.TimeUnit;

/**
 * @author zzt
 */
@ConfigurationProperties(prefix = "syncer.ack")
public class SyncerAck {

  private int flushPeriod = 100;
  private TimeUnit unit = TimeUnit.MILLISECONDS;

  public int getFlushPeriod() {
    return flushPeriod;
  }

  public void setFlushPeriod(int flushPeriod) {
    this.flushPeriod = flushPeriod;
  }

  public TimeUnit getUnit() {
    return unit;
  }

  public void setUnit(TimeUnit unit) {
    this.unit = unit;
  }
}
