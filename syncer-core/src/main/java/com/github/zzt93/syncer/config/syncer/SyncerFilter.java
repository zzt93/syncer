package com.github.zzt93.syncer.config.syncer;


import com.github.zzt93.syncer.config.consumer.output.FailureLogConfig;
import lombok.Data;

/**
 * @author zzt
 */
@Data
public class SyncerFilter {

  public static final int WORKER_THREAD_COUNT = 1;
  private int capacity = 100000;
  private FailureLogConfig failureLog = new FailureLogConfig();

  private SyncerFilterMeta filterMeta = new SyncerFilterMeta();

}
