package com.github.zzt93.syncer.consumer.output.batch;

import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.github.zzt93.syncer.config.pipeline.output.PipelineBatchConfig;
import com.github.zzt93.syncer.consumer.ack.Retryable;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zzt
 */
public class BatchBuffer<T extends Retryable> {

  private final int limit;
  private final ConcurrentLinkedDeque<T> deque = new ConcurrentLinkedDeque<>();
  private final AtomicInteger estimateSize = new AtomicInteger(0);

  public BatchBuffer(PipelineBatchConfig batch) {
    limit = batch.getSize() <= 0 ? Integer.MAX_VALUE : batch.getSize();
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
  public List<T> flushIfReachSizeLimit() {
    // The function should be side-effect-free, since it may be
    // re-applied when attempted updates fail due to contention among threads
    if (estimateSize.getAndUpdate(x -> x >= limit ? x - limit : x) >= limit) {
      ArrayList<T> res = new ArrayList<>(limit);
      for (int i = 0; !deque.isEmpty() && i < limit; i++) {
        try {
          res.add(deque.removeFirst());
        } catch (NoSuchElementException ignored) {
          // ignore, multiple thread may enter this block and cause this exception
          return res;
        }
      }
      return res;
    }
    return null;
  }

  public List<T> flush() {
    ArrayList<T> res = new ArrayList<>(estimateSize.get());
    if (estimateSize.getAndUpdate(x -> 0) > 0) {
      while (!deque.isEmpty()) {
        try {
          res.add(deque.removeFirst());
        } catch (NoSuchElementException ignored) {
          // ignore, multiple thread may enter this block and cause this exception
          return res;
        }
      }
    }
    return res;
  }
}
