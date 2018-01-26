package com.github.zzt93.syncer.config.syncer;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author zzt
 */
@Configuration
@ConfigurationProperties(prefix = "syncer")
public class SyncerConfig {

  private SyncerAck ack;
  private SyncerInput input;
  private SyncerFilter filter;
  private SyncerOutput output;

  public SyncerAck getAck() {
    return ack;
  }

  public void setAck(SyncerAck ack) {
    this.ack = ack;
  }

  public SyncerInput getInput() {
    return input;
  }

  public void setInput(SyncerInput input) {
    this.input = input;
  }

  public SyncerFilter getFilter() {
    return filter;
  }

  public void setFilter(SyncerFilter filter) {
    this.filter = filter;
  }

  public SyncerOutput getOutput() {
    return output;
  }

  public void setOutput(SyncerOutput output) {
    this.output = output;
  }
}
