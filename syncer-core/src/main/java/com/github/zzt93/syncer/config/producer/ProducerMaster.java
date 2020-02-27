package com.github.zzt93.syncer.config.producer;

import com.github.zzt93.syncer.config.ProducerConfig;
import com.github.zzt93.syncer.config.common.Connection;
import com.github.zzt93.syncer.config.common.MayClusterConnection;
import com.github.zzt93.syncer.config.consumer.input.MasterSourceType;
import lombok.Data;

/**
 * @author zzt
 */
@Data
@ProducerConfig("input.masters[]")
public class ProducerMaster {

  private MasterSourceType type = MasterSourceType.MySQL;
  private MayClusterConnection connection;
  private String file;
  private boolean onlyUpdated = true;
  private boolean updateLookUp = false;

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

}
