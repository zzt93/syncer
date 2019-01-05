package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.common.data.SyncData;

import java.util.concurrent.BlockingDeque;

/**
 * @author zzt
 */
public class DirectScheduler implements EventScheduler {

  private final BlockingDeque<SyncData>[] deque;
  private final int length;
  private int round = 0;

  DirectScheduler(BlockingDeque<SyncData>[] deques) {
    deque = deques;
    length = deques.length;
  }

  @Override
  public boolean schedule(SyncData syncData) {
    // Precondition: only one thread invoke this method
    deque[round++].addLast(syncData);
    if (round == length) {
      round = 0;
    }
    return true;
  }
}
