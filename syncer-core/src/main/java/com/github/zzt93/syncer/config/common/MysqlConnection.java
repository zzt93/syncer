package com.github.zzt93.syncer.config.common;

import com.zaxxer.hikari.HikariConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.Properties;

/**
 * @author zzt
 */
public class MysqlConnection extends Connection {
  private static final Logger logger = LoggerFactory.getLogger(MysqlConnection.class);

  public static final String DEFAULT_DB = "";

  public MysqlConnection() {
  }

  public MysqlConnection(Connection connection) {
    try {
      setAddress(connection.getAddress());
    } catch (UnknownHostException ignored) {
      logger.error("Impossible", ignored);
    }
    setPort(connection.getPort());
    setUser(connection.getUser());
    setPassword(connection.getPassword());
  }

  public String toConnectionUrl(String schema) {
    return "jdbc:mysql://" + super.toConnectionUrl(null) + "/" + schema + "?autoReconnect=true&useSSL=false&useUnicode=yes&characterEncoding=UTF-8";
  }

  private String toConnectionUrl() {
    return toConnectionUrl(DEFAULT_DB);
  }

  public HikariConfig toConfig() {
    Properties properties = new Properties();
    properties.put("jdbcUrl", toConnectionUrl());
    properties.put("username", getUser());
    properties.put("password", getPassword());
    return new HikariConfig(properties);
  }
}
