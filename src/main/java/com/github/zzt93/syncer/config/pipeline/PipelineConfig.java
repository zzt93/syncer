package com.github.zzt93.syncer.config.pipeline;

import com.github.zzt93.syncer.config.pipeline.filter.FilterConfig;
import com.github.zzt93.syncer.config.pipeline.input.Input;
import com.github.zzt93.syncer.config.pipeline.output.Output;
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

  private Input input;
  private Output output;
  private List<FilterConfig> filter = new ArrayList<>();

  public Input getInput() {
    return input;
  }

  public void setInput(Input input) {
    this.input = input;
  }

  public Output getOutput() {
    return output;
  }

  public void setOutput(Output output) {
    this.output = output;
  }

  public List<FilterConfig> getFilter() {
    return filter;
  }

  public void setFilter(List<FilterConfig> filter) {
    this.filter = filter;
  }
}

