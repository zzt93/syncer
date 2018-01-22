package com.github.zzt93.syncer.config.pipeline.common;

import java.net.UnknownHostException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class MysqlConnection extends Connection {
  private static final Logger logger = LoggerFactory.getLogger(MysqlConnection.class);

  public static final String DEFAULT_DB = "";

  public MysqlConnection(Connection connection) {
    try {
      setAddress(connection.getAddress());
    } catch (UnknownHostException ignored) {
      logger.error("Impossible", ignored);
    }
    setPort(connection.getPort());
    setUser(connection.getUser());
    setPasswordFile(connection.getPasswordFile());
  }

  public String toConnectionUrl(String schema) {
    return "jdbc:mysql://" + super.toConnectionUrl(null) + "/" + schema + "?autoReconnect=true&useSSL=false&useUnicode=yes&characterEncoding=UTF-8";
  }

  public String toConnectionUrl() {
    return toConnectionUrl(DEFAULT_DB);
  }

}
