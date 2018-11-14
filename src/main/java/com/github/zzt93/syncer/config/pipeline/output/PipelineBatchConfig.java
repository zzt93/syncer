package com.github.zzt93.syncer.config.pipeline.output;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author zzt
 */
public class PipelineBatchConfig {

  private final Logger logger = LoggerFactory.getLogger(PipelineBatchConfig.class);

  private final TimeUnit delayTimeUnit = TimeUnit.MILLISECONDS;
  /**
   * default is 100, soft limit
   */
  private int size = 100;
  /**
   * delay in {@link TimeUnit#MILLISECONDS}, default is 100
   */
  private int delay = 100;

  private int maxRetry = 5;

  /**
   * Total bytes of memory the producer can use to buffer records waiting to be sent
   * to the server.
   */
  private Long bufferMemory;

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    if (size < 0) {
      IllegalArgumentException illegalArgumentException = new IllegalArgumentException(
          "`batch.size` is invalid, minimum is 0");
      logger.error("Illegal settings of `batch.size`", illegalArgumentException);
      throw illegalArgumentException;
    }
    this.size = size;
  }

  public int getDelay() {
    return delay;
  }

  public void setDelay(int delay) {
    if (delay < 50) {
      IllegalArgumentException exception = new IllegalArgumentException(
          "Delay is too small, may affect performance. If you want to disable batch, set size to 0.");
      logger.error("Illegal settings of `batch.delay`", exception);
      throw exception;
    }
    this.delay = delay;
  }

  public int getMaxRetry() {
    return maxRetry;
  }

  public void setMaxRetry(int maxRetry) {
    this.maxRetry = maxRetry;
  }

  public TimeUnit getDelayTimeUnit() {
    return delayTimeUnit;
  }

  public Long getBufferMemory() {
    return bufferMemory;
  }

  public void setBufferMemory(Long bufferMemory) {
    this.bufferMemory = bufferMemory;
  }
}
