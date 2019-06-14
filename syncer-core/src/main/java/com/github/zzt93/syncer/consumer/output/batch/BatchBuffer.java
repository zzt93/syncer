package com.github.zzt93.syncer.consumer.output.batch;

import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.github.zzt93.syncer.config.consumer.output.PipelineBatchConfig;
import com.github.zzt93.syncer.consumer.output.Retryable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 *
 * <ul>
 *   <li>Filter thread should not share one buffer, otherwise may cause disorder: e.g.
 *   <pre>
 *    hh:ss.000: [thread2] add to buffer: insert (2, b)
 *    hh:ss.001: [thread1] add to buffer: insert (1, a)
 *    hh:ss.002: [thread1] flushIfReachSizeLimit(buffer size 2): thread1 insert (1, a)(2, b)
 *    hh:ss.003: [thread2] add to buffer: insert (4, c), update (2, d)
 *    hh:ss.004: [thread2] flushIfReachSizeLimit(buffer size 2): thread2 insert (4, c); update (2, d)
 *    // update may cause DocumentMissing, should after thread1 return
 *   </pre>
 *   </li>
 *   <li>Flush by size and time should not invoked at same time, otherwise may cause disorder: e.g.
 *   <pre>
 *    hh:ss.000: [thread1] add to buffer: insert (1, a)(2, b)
 *    hh:ss.000: [thread1] flushIfReachSizeLimit(buffer size 2): thread1 insert (1, a)(2, b)
 *    hh:ss.001: [thread2] add to buffer: update (2, d)
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

  private final Logger logger = LoggerFactory.getLogger(BatchBuffer.class);
  private final AtomicBoolean flushing = new AtomicBoolean(false);
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
    if (estimateSize.getAndUpdate(x -> !flushing.get() && x >= limit ? x - limit : x) >= limit) {
      flushing.set(true);

      ArrayList<T> res = new ArrayList<>(limit);
      for (int i = 0; i < limit; i++) {
        try {
          res.add(deque.removeFirst());
        } catch (NoSuchElementException e) {
          logger.error("Syncer Bug: {}, {}, {}", estimateSize, deque.size(), flushing, e);
          return res;
        }
      }
      return res;
    }
    return null;
  }

  public List<T> flush() {
    int size;
    if ((size = estimateSize.getAndUpdate(x -> !flushing.get() && x > 0 ? 0 : x)) > 0) {
      ArrayList<T> res = new ArrayList<>(size);
      flushing.set(true);

      for (int i = 0; i < size; i++) {
        try {
          res.add(deque.removeFirst());
        } catch (NoSuchElementException e) {
          logger.error("Syncer Bug: {}, {}, {}", estimateSize, deque.size(), flushing, e);
          return res;
        }
      }
    }
    return null;
  }

  public void flushDone() {
    flushing.set(false);
  }

}
