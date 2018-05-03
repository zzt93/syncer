package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.common.data.SyncData;
import java.util.concurrent.BlockingDeque;

/**
 * @author zzt
 */
public class HashIdScheduler implements EventScheduler {

  private final int size;
  private final BlockingDeque<SyncData>[] deques;

  public HashIdScheduler(BlockingDeque<SyncData>[] deques) {
    this.deques = deques;
    size = deques.length;
  }

  @Override
  public boolean schedule(SyncData syncData) {
    int hash = syncData.getId().hashCode();
    deques[hash % size].addLast(syncData);
    return true;
  }
}
