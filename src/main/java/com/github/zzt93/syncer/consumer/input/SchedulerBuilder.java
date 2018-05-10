package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.common.data.SyncData;
import com.google.common.base.Preconditions;
import java.util.concurrent.BlockingDeque;

/**
 * @author zzt
 */
public class SchedulerBuilder {

  private SchedulerType type = SchedulerType.hash;
  private BlockingDeque<SyncData>[] deques;

  public SchedulerBuilder() {
  }

  public SchedulerBuilder setDeques(BlockingDeque<SyncData>[] deques) {
    this.deques = deques;
    return this;
  }

  public SchedulerBuilder setSchedulerType(SchedulerType type) {
    this.type = type;
    return this;
  }

  public EventScheduler build() {
    Preconditions.checkNotNull(deques);
    switch (type) {
      case direct:
        return new DirectScheduler(deques);
      case hash:
        return new HashIdScheduler(deques);
      case mod:
      return new ModIdScheduler(deques);
      default:
        throw new IllegalStateException("Impossible");
    }
  }

  public enum SchedulerType {
    direct, hash, mod
  }

}
