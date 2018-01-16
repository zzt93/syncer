package com.github.zzt93.syncer.config.pipeline.input;

import com.github.zzt93.syncer.config.pipeline.common.MysqlConnection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class MysqlMaster {

  private Logger logger = LoggerFactory.getLogger(MysqlMaster.class);

  private MysqlConnection connection;
  private List<Schema> schemas = new ArrayList<>();
  private Set<Schema> schemaSet = new HashSet<>();

  public MysqlConnection getConnection() {
    return connection;
  }

  public void setConnection(MysqlConnection connection) {
    this.connection = connection;
  }

  public List<Schema> getSchemas() {
    return schemas;
  }

  public Set<Schema> getSchemaSet() {
    return schemaSet;
  }

  public void setSchemas(List<Schema> schemas) {
    this.schemas = schemas;
    schemaSet.addAll(schemas);
    if (schemaSet.size() < schemas.size()) {
      logger.warn("Duplicate schemas in settings: {}", schemas);
    }
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
