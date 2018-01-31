package com.github.zzt93.syncer.config.pipeline.output;

import java.util.concurrent.TimeUnit;

/**
 * @author zzt
 */
public class FailureLogConfig {

  private int countLimit = 1000;
  private int timeLimit = 60;
  private TimeUnit unit = TimeUnit.SECONDS;

  public int getCountLimit() {
    return countLimit;
  }

  public void setCountLimit(int countLimit) {
    this.countLimit = countLimit;
  }

  public int getTimeLimit() {
    return timeLimit;
  }

  public void setTimeLimit(int timeLimit) {
    this.timeLimit = timeLimit;
  }

  public TimeUnit getUnit() {
    return unit;
  }

  public void setUnit(TimeUnit unit) {
    this.unit = unit;
  }
}
