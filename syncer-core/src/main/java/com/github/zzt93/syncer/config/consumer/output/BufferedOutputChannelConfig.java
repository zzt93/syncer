package com.github.zzt93.syncer.config.consumer.output;

import com.github.zzt93.syncer.config.ConsumerConfig;
import lombok.Getter;
import lombok.Setter;

/**
 * @author zzt
 */
@Getter
@Setter
public abstract class BufferedOutputChannelConfig implements OutputChannelConfig {

  @ConsumerConfig
  private PipelineBatchConfig batch = new PipelineBatchConfig();
  @ConsumerConfig
  private FailureLogConfig failureLog = new FailureLogConfig();

}
