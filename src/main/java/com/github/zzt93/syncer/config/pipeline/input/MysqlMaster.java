package com.github.zzt93.syncer.config.pipeline.input;

import com.github.zzt93.syncer.config.pipeline.common.MysqlConnection;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zzt
 */
public class MysqlMaster {

  private MysqlConnection connection;
  private List<Schema> schemas = new ArrayList<>();

  public MysqlConnection getConnection() {
    return connection;
  }

  public void setConnection(MysqlConnection connection) {
    this.connection = connection;
  }

  public List<Schema> getSchemas() {
    return schemas;
  }

  public MysqlMaster setSchemas(List<Schema> schemas) {
    this.schemas = schemas;
    return this;
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
        ", schemas=" + schemas +
        '}';
  }

}
