package com.github.zzt93.syncer.config.pipeline.common;

/**
 * @author zzt
 */
public class MysqlConnection extends Connection {

  public static final String DEFAULT_DB = "";

  public String toConnectionUrl(String schema) {
    return "jdbc:mysql://" + super.toConnectionUrl(null) + "/" + schema + "?autoReconnect=true&useSSL=false";
  }

  public String toConnectionUrl() {
    return toConnectionUrl(DEFAULT_DB);
  }
}
