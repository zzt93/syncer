package com.github.zzt93.syncer.config.syncer;


import lombok.Data;

/**
 * @author zzt
 */
@Data
public class SyncerFilter {

  public static final int WORKER_THREAD_COUNT = 1;

  private SyncerFilterMeta filterMeta = new SyncerFilterMeta();

}
