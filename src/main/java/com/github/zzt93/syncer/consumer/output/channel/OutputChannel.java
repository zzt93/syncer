package com.github.zzt93.syncer.consumer.output.channel;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.thread.ThreadSafe;
import java.util.List;

/**
 * @author zzt
 */
public interface OutputChannel {

  /**
   * Should be thread safe; Should retry if failed
   *
   * @param event the data from filter module
   * @return whether output is success
   */
  @ThreadSafe
  boolean output(SyncData event);

  @ThreadSafe
  boolean output(List<SyncData> batch);

  String des();


}
