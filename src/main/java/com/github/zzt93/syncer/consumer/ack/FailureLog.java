package com.github.zzt93.syncer.consumer.ack;

import com.github.zzt93.syncer.common.data.SyncWrapper;
import com.github.zzt93.syncer.common.exception.FailureException;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zzt
 */
public class FailureLog<T> {

  private AtomicInteger itemCount = new AtomicInteger(0);
  private int limit;

  public FailureLog(Path path) {
    limit = 1000;
    List<SyncWrapper<T>> recover = recover(path);
  }

  private List<SyncWrapper<T>> recover(Path path) {
    List<SyncWrapper<T>> res = new LinkedList<>();
    return res;
  }

  public boolean log(SyncWrapper<T> syncWrapper) {
    itemCount.incrementAndGet();
    if (itemCount.get() > limit) {
      throw new FailureException("Too many failed items, abort and need human influence");
    }
    return true;
  }

}
