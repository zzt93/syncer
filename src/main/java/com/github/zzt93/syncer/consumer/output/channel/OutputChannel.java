package com.github.zzt93.syncer.consumer.output.channel;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.thread.ThreadSafe;
import java.io.Closeable;

/**
 * @author zzt
 */
public interface OutputChannel extends Closeable {

  /**
   * Should be thread safe; Should retry if failed
   *
   * @param event the data from filter module
   * @return whether output is success
   */
  @ThreadSafe
  boolean output(SyncData event) throws InterruptedException;

  String des();

  void close();

}
