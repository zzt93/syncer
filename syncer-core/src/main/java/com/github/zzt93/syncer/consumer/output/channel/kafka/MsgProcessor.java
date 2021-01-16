package com.github.zzt93.syncer.consumer.output.channel.kafka;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.consumer.output.kafka.MsgMapping;

/**
 * @author zzt
 */
public class MsgProcessor {

  private final boolean includeBefore;

  MsgProcessor(MsgMapping mapping) {
    includeBefore = mapping.isIncludeBefore();
  }

  void process(SyncData syncData) {
    if (!includeBefore) {
      syncData.getResult().setBefore(null);
    }
  }
}
