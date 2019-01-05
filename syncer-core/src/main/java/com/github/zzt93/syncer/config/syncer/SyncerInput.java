package com.github.zzt93.syncer.config.syncer;


/**
 * @author zzt
 */
public class SyncerInput {

  private int maxRetry;
  private SyncerInputMeta inputMeta = new SyncerInputMeta();

  public SyncerInputMeta getInputMeta() {
    return inputMeta;
  }

  public void setInputMeta(SyncerInputMeta inputMeta) {
    this.inputMeta = inputMeta;
  }

  public int getMaxRetry() {
    return maxRetry;
  }

  public void setMaxRetry(int maxRetry) {
    this.maxRetry = maxRetry;
  }
}
