package com.github.zzt93.syncer.config.producer;

import com.github.zzt93.syncer.config.consumer.common.Connection;
import com.github.zzt93.syncer.config.consumer.input.MasterSourceType;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author zzt
 */
@ConfigurationProperties("syncer.producer.input.masters[]")
public class ProducerMaster {

  private MasterSourceType type = MasterSourceType.MySQL;
  private Connection connection;
  private String file;

  public Connection getConnection() {
    return connection;
  }

  public void setConnection(Connection connection) {
    this.connection = connection;
  }

  public MasterSourceType getType() {
    return type;
  }

  public void setType(MasterSourceType type) {
    this.type = type;
  }

  public String getFile() {
    return file;
  }

  public void setFile(String file) {
    this.file = file;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ProducerMaster that = (ProducerMaster) o;

    return connection.equals(that.connection);
  }

  @Override
  public int hashCode() {
    return connection.hashCode();
  }

  @Override
  public String toString() {
    return "ProducerMaster{" +
        "type=" + type +
        ", connection=" + connection +
        ", file='" + file + '\'' +
        '}';
  }
}
