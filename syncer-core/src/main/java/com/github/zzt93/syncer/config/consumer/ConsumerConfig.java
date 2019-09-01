package com.github.zzt93.syncer.config.consumer;

import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.consumer.filter.FilterConfig;
import com.github.zzt93.syncer.config.consumer.input.PipelineInput;
import com.github.zzt93.syncer.config.consumer.output.PipelineOutput;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zzt
 */
public class ConsumerConfig {

  private String version;
  private String consumerId;
  private PipelineInput input;
  private PipelineOutput output;
  private List<FilterConfig> filter = new ArrayList<>();

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getConsumerId() {
    return consumerId;
  }

  public void setConsumerId(String consumerId) {
    this.consumerId = consumerId;
  }

  public PipelineInput getInput() {
    return input;
  }

  public void setInput(PipelineInput pipelineInput) {
    this.input = pipelineInput;
  }

  public PipelineOutput getOutput() {
    return output;
  }

  public void setOutput(PipelineOutput pipelineOutput) {
    this.output = pipelineOutput;
  }

  public List<FilterConfig> getFilter() {
    return filter;
  }

  public void setFilter(List<FilterConfig> filter) {
    if (filter == null) {
      throw new InvalidConfigException("No filter config content, but has `filter` key");
    }
    this.filter = filter;
  }

  public int outputSize() {
    return output.outputChannels();
  }
}

