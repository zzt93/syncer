package com.github.zzt93.syncer.producer.output;

import com.github.zzt93.syncer.common.SyncData;
import java.util.List;

/**
 * @author zzt
 */
public interface OutputSink {

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

  List<String> accept();
}
