package com.github.zzt93.syncer.config.syncer;


/**
 * @author zzt
 */
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
