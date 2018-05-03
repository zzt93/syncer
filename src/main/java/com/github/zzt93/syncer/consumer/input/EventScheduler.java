package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.common.data.SyncData;

/**
 * @author zzt
 */
public interface EventScheduler {

  boolean schedule(SyncData syncData);

}
