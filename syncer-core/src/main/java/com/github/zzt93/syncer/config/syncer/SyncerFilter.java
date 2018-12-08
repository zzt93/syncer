package com.github.zzt93.syncer.config.syncer;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author zzt
 */
@ConfigurationProperties(prefix = "syncer.filter")
public class SyncerFilter {

  private int worker;
  private SyncerFilterMeta filterMeta = new SyncerFilterMeta();

  public SyncerFilterMeta getFilterMeta() {
    return filterMeta;
  }

  public void setFilterMeta(SyncerFilterMeta filterMeta) {
    this.filterMeta = filterMeta;
  }

  public int getWorker() {
    return worker;
  }

  public void setWorker(int worker) {
    this.worker = worker;
  }
}
