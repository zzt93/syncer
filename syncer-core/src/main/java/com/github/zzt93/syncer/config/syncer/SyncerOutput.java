package com.github.zzt93.syncer.config.syncer;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author zzt
 */
@ConfigurationProperties(prefix = "syncer.output")
public class SyncerOutput {

  private int worker;
  private SyncerBatch batch;
  private SyncerOutputMeta outputMeta;

  public SyncerOutputMeta getOutputMeta() {
    return outputMeta;
  }

  public void setOutputMeta(SyncerOutputMeta outputMeta) {
    this.outputMeta = outputMeta;
  }

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
