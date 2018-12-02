package com.github.zzt93.syncer.config.syncer;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author zzt
 */
@ConfigurationProperties(prefix = "syncer.output.batch")
public class SyncerBatch {

  private int worker;

  public int getWorker() {
    return worker;
  }

  public void setWorker(int worker) {
    this.worker = worker;
  }
}
