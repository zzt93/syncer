package com.github.zzt93.syncer.config.consumer;

import com.github.zzt93.syncer.config.common.MasterSource;
import com.github.zzt93.syncer.config.consumer.filter.FilterConfig;
import com.github.zzt93.syncer.config.consumer.input.PipelineInput;
import com.github.zzt93.syncer.config.consumer.output.PipelineOutput;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.List;

/**
 * @author zzt
 */
@Slf4j
@Getter
public class ConsumerConfig {

  public String version;
  public String consumerId;
  public List<MasterSource> input;
  public PipelineOutput output;
  @com.github.zzt93.syncer.config.ConsumerConfig("filter")
  public FilterConfig filter = new FilterConfig();

  public PipelineInput getInput() {
    HashSet<MasterSource> inputSet = new HashSet<>(input);
    if (inputSet.size() < input.size()) {
      log.error("Duplicate master source: {}", input);
    }
    return new PipelineInput(inputSet);
  }

  public int outputSize() {
    return output.outputChannels();
  }
}

