package com.github.zzt93.syncer.producer.input.mysql.connect;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.network.ServerException;
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
    if (binlogDeprecated(ex)) {
      if (dupServerId(ex)) {
        throw new DupServerIdException(ex);
      }
      throw new InvalidBinlogException(ex, client.getBinlogFilename(), client.getBinlogPosition());
    }
  }

  private boolean dupServerId(Exception ex) {
    return ex.getMessage().startsWith("A slave with the same server_uuid/server_id as this slave has connected to the master");
  }

  private boolean binlogDeprecated(Exception ex) {
    return ex instanceof ServerException && ((ServerException) ex).getErrorCode() == 1236;
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
