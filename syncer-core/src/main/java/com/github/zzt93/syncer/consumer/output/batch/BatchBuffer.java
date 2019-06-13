package com.github.zzt93.syncer.consumer.output.batch;

import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.github.zzt93.syncer.config.consumer.output.PipelineBatchConfig;
import com.github.zzt93.syncer.consumer.output.Retryable;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 *
 * <ul>
 *   <li>Filter thread should not share one buffer, otherwise may cause disorder: e.g.
 *   <pre>
 *    hh:ss.000: [thread1] insert (1, a)
 *    hh:ss.001: [thread2] insert (2, b), insert (3, c), update (2, d)
 *    hh:ss.002: [thread1] flushIfReachSizeLimit(buffer size 2): thread1 insert (1, a)(2, b)
 *    hh:ss.003: [thread2] flushIfReachSizeLimit(buffer size 2): thread2 insert (3, c); update (2, d)
 *    // update may cause DocumentMissing, should after thread1 return
 *   </pre>
 *   </li>
 *   <li>Flush by size and time should not invoked at same time, otherwise may cause disorder: e.g.
 *   <pre>
 *    hh:ss.000: [thread1] buffer: (1, a)(2, b)
 *    hh:ss.000: [thread1] flushIfReachSizeLimit(buffer size 2): thread1 insert (1, a)(2, b)
 *    hh:ss.001: [thread2] add to buffer: (2, d)
 *    hh:ss.002: [thread2] flush(time reach): thread2 update (2, d)
 *   </pre>
 *   </li>
 * </ul>
 *
 * <h3>Solution</h3>
 *
 * <ul>
 *   <li>Every filter thread should have one buffer</li>
 *   <li>A flush timer thread flush all filter buffer</li>
 *   <li>A buffer have a `flushing` flag (set when `flushing == false` in flushXXX, unSet when finish remote request),
 *   a flushing buffer refuse other flush request</li>
 * </ul>
 *
 *  @author zzt
 */
public class BatchBuffer<T extends Retryable> {

  private final int limit;
  /**
   * <h3>Equation</h3>
   * deque.size() >= estimateSize
   */
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

  public boolean addAllInHead(List<T> data) {
    for (T datum : data) {
      datum.inc();
      deque.addFirst(datum);
    }
    estimateSize.addAndGet(data.size());
    return true;
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
          // TODO 2019/3/13 should not flush & flushIfReachSizeLimit at the same time, fix it by: volatile boolean flushing?
          // TODO 2019/3/13 should not ignore
          return res;
        }
      }
      return res;
    }
    return null;
  }

  public List<T> flush() {
    // TODO 2019/3/13 move into if
    ArrayList<T> res = new ArrayList<>(estimateSize.get());
    if (estimateSize.getAndUpdate(x -> 0) > 0) {
      while (!deque.isEmpty()) {
        try {
          res.add(deque.removeFirst());
        } catch (NoSuchElementException ignored) {
          // TODO 2019/3/13 should not ignore
          return res;
        }
      }
      // TODO 2019/3/13 reset estimateSize to 0?
    }
    return res;
  }
}
