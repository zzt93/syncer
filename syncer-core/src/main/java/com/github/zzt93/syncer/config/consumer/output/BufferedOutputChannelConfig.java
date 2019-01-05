package com.github.zzt93.syncer.config.consumer.output;

/**
 * @author zzt
 */
public abstract class BufferedOutputChannelConfig implements OutputChannelConfig {

  private PipelineBatchConfig batch = new PipelineBatchConfig();
  private FailureLogConfig failureLog = new FailureLogConfig();

  public FailureLogConfig getFailureLog() {
    return failureLog;
  }

  public void setFailureLog(FailureLogConfig failureLog) {
    this.failureLog = failureLog;
  }

  public PipelineBatchConfig getBatch() {
    return batch;
  }

  public void setBatch(PipelineBatchConfig batch) {
    this.batch = batch;
  }

}
