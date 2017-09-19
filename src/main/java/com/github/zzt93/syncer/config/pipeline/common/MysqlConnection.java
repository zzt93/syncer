package com.github.zzt93.syncer.config.pipeline.common;

/**
 * @author zzt
 */
public class MysqlConnection extends Connection {

  public String toConnectionUrl(String schema) {
    return "jdbc:mysql://" + super.toConnectionUrl(null) + "/" + schema;
  }
}
