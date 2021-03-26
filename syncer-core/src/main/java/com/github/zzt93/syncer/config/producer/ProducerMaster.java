package com.github.zzt93.syncer.config.producer;

import com.github.zzt93.syncer.config.ProducerConfig;
import com.github.zzt93.syncer.config.common.Connection;
import com.github.zzt93.syncer.config.common.MayClusterConnection;
import com.github.zzt93.syncer.config.consumer.input.MasterSourceType;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author zzt
 */
@Data
@ProducerConfig("input[]")
public class ProducerMaster {

  private MasterSourceType type = MasterSourceType.MySQL;
  private MayClusterConnection connection;
  private String file;
  private boolean onlyUpdated = true;
  private boolean updateLookUp = false;
  private boolean bsonConversion = true;

  public Connection getRealConnection() {
    connection.validate(type);
    return connection.getRealConnection();
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

  public MongoV4Option mongoV4Option() {
    return new MongoV4Option(updateLookUp, bsonConversion);
  }

  @Data
  @AllArgsConstructor
  public static class MongoV4Option {
    private boolean updateLookUp;
    private boolean bsonConversion;
  }
}
