package com.github.zzt93.syncer.config.syncer;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author zzt
 */
@Configuration
@ConfigurationProperties(prefix = "syncer")
public class SyncerConfig {

  private InputModule input;
  private FilterModule filter;
  private OutputModule output;

  public InputModule getInput() {
    return input;
  }

  public void setInput(InputModule input) {
    this.input = input;
  }

  public FilterModule getFilter() {
    return filter;
  }

  public void setFilter(FilterModule filter) {
    this.filter = filter;
  }

  public OutputModule getOutput() {
    return output;
  }

  public void setOutput(OutputModule output) {
    this.output = output;
  }
}
