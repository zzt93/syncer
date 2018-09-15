package com.github.zzt93.syncer.config.syncer;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.server.ConfigurableServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author zzt
 */
@Configuration
@ConfigurationProperties(prefix = "syncer")
public class SyncerConfig {

  private int port;
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

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  @Bean
  public ConfigurableServletWebServerFactory webServerFactory() {
    TomcatServletWebServerFactory factory = new TomcatServletWebServerFactory();
    if (port != 0) {
      factory.setPort(port);
    }
    return factory;
  }

}
