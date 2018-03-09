package com.github.zzt93.syncer.config.syncer;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author zzt
 */
@ConfigurationProperties(prefix = "syncer.input")
public class SyncerInput {

  private int worker;
  private int maxRetry;
  private SyncerInputMeta inputMeta;

  public SyncerInputMeta getInputMeta() {
    return inputMeta;
  }

  public void setInputMeta(SyncerInputMeta inputMeta) {
    this.inputMeta = inputMeta;
  }

  public int getWorker() {
    return worker;
  }

  public void setWorker(int worker) {
    this.worker = worker;
  }

  public int getMaxRetry() {
    return maxRetry;
  }

  public void setMaxRetry(int maxRetry) {
    this.maxRetry = maxRetry;
  }
}
