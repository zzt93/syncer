package com.github.zzt93.syncer.output.channel;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.output.OutputJob;
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
  boolean output(SyncData event);

  boolean output(List<SyncData> batch);

  String des();


}
