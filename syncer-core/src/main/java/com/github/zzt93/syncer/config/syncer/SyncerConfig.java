package com.github.zzt93.syncer.config.syncer;

import com.github.zzt93.syncer.config.common.InvalidConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class SyncerConfig {

  private static final String SERVER_PORT = "port";
  private static final Logger logger = LoggerFactory.getLogger(SyncerConfig.class);
  private static final int DEFAULT_START = 40000;
  private static final String RETRY = "10";

  private String version;
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

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }
}
