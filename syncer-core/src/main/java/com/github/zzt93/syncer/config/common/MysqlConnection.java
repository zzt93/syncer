package com.github.zzt93.syncer.config.common;

import com.mysql.cj.jdbc.Driver;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.net.UnknownHostException;
import java.util.Properties;

/**
 * @author zzt
 */
@NoArgsConstructor
public class MysqlConnection extends Connection {
  private static final Logger logger = LoggerFactory.getLogger(MysqlConnection.class);

  public static final String DEFAULT_DB = "";

  public MysqlConnection(String addr, int port, String user, String pass) throws UnknownHostException {
    setAddress(addr);
    setPort(port);
    setUser(user);
    setPassword(pass);
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

  public DataSource dataSource() {
    String className = Driver.class.getName();
    HikariConfig config = toConfig();
    config.setDriverClassName(className);
    // A value less than zero will not bypass any connection attempt and validation during startup,
    // and therefore the pool will start immediately
    config.setInitializationFailTimeout(-1);
    return new HikariDataSource(config);
  }
}
