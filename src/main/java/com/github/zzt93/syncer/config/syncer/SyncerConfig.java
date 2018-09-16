package com.github.zzt93.syncer.config.syncer;

import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.web.embedded.tomcat.TomcatConnectorCustomizer;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.server.ConfigurableServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;

/**
 * @author zzt
 */
@Configuration
@ConfigurationProperties(prefix = "syncer")
public class SyncerConfig {

  private static final String SERVER_PORT = "port";
  private static final Logger logger = LoggerFactory.getLogger(SyncerConfig.class);
  private static final int DEFAULT_START = 10000;
  private static final String RETRY = "10";

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
    if (port <= 0 || port > 65535) throw new InvalidConfigException("Invalid port config " + port);
    this.port = port;
  }

  @Bean
  public ConfigurableServletWebServerFactory webServerFactory(ConfigurableEnvironment environment) {
    String property = environment.getProperty(SERVER_PORT);
    TomcatServletWebServerFactory factory = new TomcatServletWebServerFactory();
    int port = DEFAULT_START;
    if (property != null) {
      port = Integer.parseInt(property);
    }  else {
      if (this.port != 0) {
        port = this.port;
      } else {
        factory.setProtocol("com.github.zzt93.syncer.health.export.ReconnectProtocol");
        factory.setTomcatConnectorCustomizers(Lists.newArrayList((TomcatConnectorCustomizer) connector -> {
          connector.setProperty("retry", RETRY);
        }));
      }
    }
    logger.info("Starting server at {}", port);
    factory.setPort(port);
    return factory;
  }

}
