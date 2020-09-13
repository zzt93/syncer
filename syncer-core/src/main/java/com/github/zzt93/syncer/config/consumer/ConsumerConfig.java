package com.github.zzt93.syncer.config.consumer;

import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.common.MasterSource;
import com.github.zzt93.syncer.config.consumer.filter.FilterConfig;
import com.github.zzt93.syncer.config.consumer.input.PipelineInput;
import com.github.zzt93.syncer.config.consumer.output.PipelineOutput;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author zzt
 */
@Slf4j
@Getter
@Setter
public class ConsumerConfig {

  private String version;
  private String consumerId;
//  private List<MasterSource> input;
  private Set<MasterSource> inputSet = new HashSet<>();
  private PipelineOutput output;
  private List<FilterConfig> filter = new ArrayList<>();

  public PipelineInput getInput() {
    return new PipelineInput(inputSet);
  }

  public void setInput(List<MasterSource> input) {
    inputSet.addAll(input);
    if (inputSet.size() < input.size()) {
      log.error("Duplicate master source: {}", input);
      throw new InvalidConfigException("Duplicate master source");
    }
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

