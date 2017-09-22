package com.github.zzt93.syncer.config.pipeline;

import com.github.zzt93.syncer.config.pipeline.filter.FilterConfig;
import com.github.zzt93.syncer.config.pipeline.input.PipelineInput;
import com.github.zzt93.syncer.config.pipeline.output.PipelineOutput;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author zzt
 */
//@PropertySource("classpath:syncer.properties") // not work with yml file!
@Configuration
@ConfigurationProperties(prefix = "pipeline")
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

