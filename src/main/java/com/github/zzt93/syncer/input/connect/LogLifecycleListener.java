package com.github.zzt93.syncer.input.connect;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class LogLifecycleListener implements BinaryLogClient.LifecycleListener {

  private Logger logger = LoggerFactory.getLogger(LogLifecycleListener.class);

  @Override
  public void onConnect(BinaryLogClient client) {
    logger.info("Connected {}@{}", client.getBinlogFilename(), client.getBinlogPosition());
  }

  @Override
  public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
    logger.error("Communication failure", ex);
  }

  @Override
  public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
    logger.error("Deserialization failure", ex);
  }

  @Override
  public void onDisconnect(BinaryLogClient client) {
    logger.warn("Disconnect {}@{}", client.getBinlogFilename(), client.getBinlogPosition());
  }
}
