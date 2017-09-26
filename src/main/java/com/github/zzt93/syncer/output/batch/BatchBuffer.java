package com.github.zzt93.syncer.output.batch;

import com.github.zzt93.syncer.common.ThreadSafe;
import com.github.zzt93.syncer.config.pipeline.output.PipelineBatch;
import java.lang.reflect.Array;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author zzt
 */
public class BatchBuffer<T> {

  private final int limit;
  private final BlockingDeque<T> blockingDeque = new LinkedBlockingDeque<>();
  private final T[] clazz;

  public BatchBuffer(PipelineBatch batch,
      Class<T> aClass) {
    limit = batch.getSize();
    clazz = (T[]) Array.newInstance(aClass, 0);
  }

  public boolean add(T data) {
    return blockingDeque.offer(data);
  }

  public boolean addAll(List<T> data) {
    return blockingDeque.addAll(data);
  }

  @ThreadSafe
  public T[] flushIfReachSizeLimit() {
    synchronized (blockingDeque) {
      if (limit <= blockingDeque.size()) {
        T[] res = blockingDeque.toArray(clazz);
        blockingDeque.clear();
        return res;
      }
    }
    return null;
  }

  public T[] flush() {
    synchronized (blockingDeque) {
      T[] res = blockingDeque.toArray(clazz);
      blockingDeque.clear();
      return res;
    }
  }
}
