package com.github.zzt93.syncer.config.pipeline.common;

import java.sql.SQLException;

/**
 * @author zzt
 */
public class SchemaUnavailableException extends SQLException {

  public SchemaUnavailableException(SQLException e) {
    super(e);
  }
}
