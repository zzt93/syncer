package com.github.zzt93.syncer.output.batch;

import com.github.zzt93.syncer.config.pipeline.output.PipelineBatch;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author zzt
 */
public class BatchBuffer<T> {

  private final int limit;
  private final BlockingDeque<T> blockingDeque = new LinkedBlockingDeque<>();

  public BatchBuffer(PipelineBatch batch) {
    limit = batch.getSize();
  }

  public boolean add(T data) {
    return blockingDeque.offer(data);
  }

  public boolean addAll(List<T> data) {
    return blockingDeque.addAll(data);
  }

  public void flush() {

  }

  public boolean reachLimit() {
    return limit <= blockingDeque.size();
  }
}
