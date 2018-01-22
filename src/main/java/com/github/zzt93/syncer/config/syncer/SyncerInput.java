package com.github.zzt93.syncer.config.syncer;

/**
 * @author zzt
 */
public class SyncerInput {

  private int worker;
  private int maxRetry;
  private SyncerMeta inputMeta;

  public SyncerMeta getInputMeta() {
    return inputMeta;
  }

  public void setInputMeta(SyncerMeta inputMeta) {
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
