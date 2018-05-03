package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.common.data.SyncData;
import java.util.concurrent.BlockingDeque;

/**
 * @author zzt
 */
public class DirectScheduler implements EventScheduler {

  private final BlockingDeque<SyncData> deque;

  DirectScheduler(BlockingDeque<SyncData>[] deques) {
    deque = deques[0];
    for (int i = 1; i < deques.length; i++) {
      deques[i] = null;
    }
  }

  @Override
  public boolean schedule(SyncData syncData) {
    deque.addLast(syncData);
    return true;
  }
}
