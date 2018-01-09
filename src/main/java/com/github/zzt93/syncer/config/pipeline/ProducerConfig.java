package com.github.zzt93.syncer.config.pipeline;

import com.github.zzt93.syncer.config.pipeline.input.PipelineInput;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author zzt
 */
@Configuration
@ConfigurationProperties
public class ProducerConfig {

  private PipelineInput input;

  public PipelineInput getInput() {
    return input;
  }

  public void setInput(PipelineInput input) {
    this.input = input;
  }

}
