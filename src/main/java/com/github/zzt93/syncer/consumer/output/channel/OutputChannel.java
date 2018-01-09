package com.github.zzt93.syncer.consumer.output.channel;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.ThreadSafe;
import com.github.zzt93.syncer.consumer.output.OutputJob;
import java.util.List;

/**
 * @author zzt
 */
public interface OutputChannel {

  /**
   * Should be thread safe
   * @param event the data from filter module
   * @return whether output is success
   *
   * @see OutputJob#call()
   */
  @ThreadSafe
  boolean output(SyncData event);

  @ThreadSafe
  boolean output(List<SyncData> batch);

  String des();


}
