package com.github.zzt93.syncer.consumer.output.batch;

import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.github.zzt93.syncer.config.pipeline.output.PipelineBatch;
import com.github.zzt93.syncer.consumer.ack.Retryable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zzt
 */
public class BatchBuffer<T extends Retryable> {

  private final int limit;
  private final ConcurrentLinkedDeque<T> deque = new ConcurrentLinkedDeque<>();
  private final AtomicInteger estimateSize = new AtomicInteger(0);
  private final Class<T> clazz;

  public BatchBuffer(PipelineBatch batch, Class<T> aClass) {
    limit = batch.getSize();
    clazz = aClass;
  }

  public boolean add(T data) {
    data.inc();
    deque.addLast(data);
    estimateSize.incrementAndGet();
    return true;
  }

  public void addFirst(T data) {
    data.inc();
    deque.addFirst(data);
    estimateSize.incrementAndGet();
  }

  public boolean addAll(List<T> data) {
    data.forEach(T::inc);
    boolean res = deque.addAll(data);
    estimateSize.addAndGet(data.size());
    return res;
  }

  @ThreadSafe(safe = {ConcurrentLinkedDeque.class, AtomicInteger.class})
  public T[] flushIfReachSizeLimit() {
    // The function should be side-effect-free, since it may be
    // re-applied when attempted updates fail due to contention among threads
    if (estimateSize.getAndUpdate(x -> x >= limit ? x - limit : x) >= limit) {
      T[] res = (T[]) Array.newInstance(clazz, limit);
      for (int i = 0; !deque.isEmpty() && i < limit; i++) {
        res[i] = deque.removeFirst();
      }
      return res;
    }
    return null;
  }

  public T[] flush() {
    ArrayList<T> res = new ArrayList<>();
    if (estimateSize.getAndUpdate(x -> 0) > 0) {
      while (!deque.isEmpty()) {
        res.add(deque.removeFirst());
      }
    }
    return res.toArray((T[]) Array.newInstance(clazz, 0));
  }
}
