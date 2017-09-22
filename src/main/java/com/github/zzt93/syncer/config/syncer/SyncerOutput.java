package com.github.zzt93.syncer.config.syncer;

/**
 * @author zzt
 */
public class SyncerOutput {

  private int worker;
  private SyncerBatch batch;

  public SyncerBatch getBatch() {
    return batch;
  }

  public void setBatch(SyncerBatch batch) {
    this.batch = batch;
  }

  public int getWorker() {
    return worker;
  }

  public void setWorker(int worker) {
    this.worker = worker;
  }
}
