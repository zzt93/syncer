package com.github.zzt93.syncer.config.pipeline.input;

import com.github.zzt93.syncer.config.pipeline.common.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class MasterSource {

  private final Logger logger = LoggerFactory.getLogger(MasterSource.class);

  private MasterSourceType type = MasterSourceType.MySQL;
  private Connection connection;
  private List<Schema> schemas = new ArrayList<>();
  private final Set<Schema> schemaSet = new HashSet<>();

  public Connection getConnection() {
    return connection;
  }

  public void setConnection(Connection connection) {
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

  public MasterSourceType getType() {
    return type;
  }

  public void setType(MasterSourceType type) {
    this.type = type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MasterSource that = (MasterSource) o;

    return connection.equals(that.connection);
  }

  @Override
  public int hashCode() {
    return connection.hashCode();
  }

  @Override
  public String toString() {
    return "MasterSource{" +
        "connection=" + connection +
        ", schemas=" + schemas +
        '}';
  }

}
