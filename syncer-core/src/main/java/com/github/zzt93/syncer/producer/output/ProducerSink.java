package com.github.zzt93.syncer.producer.output;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.consumer.ConsumerSource;

import java.util.Collection;

/**
 * @author zzt
 */
public interface ProducerSink {

  /**
   * @param data SyncData info
   * @return whether the data reach the consumer
   */
  boolean output(SyncData data);

  /**
   * The validness of this method is ensured by all insert/update/delete rows
   * in a single row-based logging event is from a single statement and to a
   * single table
   * @param data data from a single event
   * @return whether the data reach the consumer
   */
  boolean output(SyncData[] data);

  boolean output(Collection<SyncData> data);

  ConsumerSource remote();

  String toString();
}
