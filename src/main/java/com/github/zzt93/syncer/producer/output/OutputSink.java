package com.github.zzt93.syncer.producer.output;

import com.github.zzt93.syncer.common.SyncData;

/**
 * @author zzt
 */
public interface OutputSink {

  /**
   * @param data SyncData info
   * @return whether the data reach the consumer
   */
  boolean output(SyncData data);

  boolean output(SyncData[] data);

  String clientId();

}
