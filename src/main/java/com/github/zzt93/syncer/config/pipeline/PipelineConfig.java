package com.github.zzt93.syncer.config.pipeline;

import com.github.zzt93.syncer.config.pipeline.filter.FilterConfig;
import com.github.zzt93.syncer.config.pipeline.input.PipelineInput;
import com.github.zzt93.syncer.config.pipeline.output.PipelineOutput;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zzt
 */
public class PipelineConfig {

  private PipelineInput input;
  private PipelineOutput output;
  private List<FilterConfig> filter = new ArrayList<>();

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
    this.filter = filter;
  }
}

