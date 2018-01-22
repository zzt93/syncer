package com.github.zzt93.syncer.config.pipeline.common;

import java.net.UnknownHostException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class MongoConnection extends Connection {
  private static final Logger logger = LoggerFactory.getLogger(MongoConnection.class);

  public MongoConnection(Connection connection) {
    try {
      setAddress(connection.getAddress());
    } catch (UnknownHostException ignored) {
      logger.error("Impossible", ignored);
    }
    setPort(connection.getPort());
    setUser(connection.getUser());
    setPasswordFile(connection.getPasswordFile());
  }

  @Override
  public String toConnectionUrl(String path) {
    return "mongodb://" + super.toConnectionUrl(null);
  }
}
