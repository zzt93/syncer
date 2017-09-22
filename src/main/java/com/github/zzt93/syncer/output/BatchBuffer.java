package com.github.zzt93.syncer.output;

import com.github.zzt93.syncer.config.pipeline.output.PipelineBatch;
import java.util.List;

/**
 * @author zzt
 */
public class BatchBuffer<T> {

  private final int limit;
  private final int delay;

  public BatchBuffer(PipelineBatch batch) {
    limit = batch.getSize();
    delay = batch.getDelay();
  }

  public boolean add(T data) {

    return true;
  }

  public boolean addAll(List<T> data) {

    return true;
  }


  private void flush() {

  }
}
