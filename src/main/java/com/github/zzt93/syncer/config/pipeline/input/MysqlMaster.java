package com.github.zzt93.syncer.config.pipeline.input;

import com.github.zzt93.syncer.config.pipeline.common.MysqlConnection;

/**
 * @author zzt
 */
public class MysqlMaster {

  private MysqlConnection connection;
  private Schema schema;

  public MysqlConnection getConnection() {
    return connection;
  }

  public void setConnection(MysqlConnection connection) {
    this.connection = connection;
  }

  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  @Override
  public boolean equals(Object o) {
      if (this == o) {
          return true;
      }
      if (o == null || getClass() != o.getClass()) {
          return false;
      }

    MysqlMaster that = (MysqlMaster) o;

    return connection.equals(that.connection);
  }

  @Override
  public int hashCode() {
    return connection.hashCode();
  }

  @Override
  public String toString() {
    return "MysqlMaster{" +
        "connection=" + connection +
        ", schema=" + schema +
        '}';
  }

}
