package com.github.zzt93.syncer.config.consumer;

import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.common.MasterSource;
import com.github.zzt93.syncer.config.consumer.filter.FilterConfig;
import com.github.zzt93.syncer.config.consumer.input.PipelineInput;
import com.github.zzt93.syncer.config.consumer.output.PipelineOutput;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
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
  private List<FilterConfig> filter = new ArrayList<>();

  public PipelineInput getInput() {
    HashSet<MasterSource> inputSet = new HashSet<>(input);
    if (inputSet.size() < input.size()) {
      log.error("Duplicate master source: {}", input);
    }
    return new PipelineInput(inputSet);
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

