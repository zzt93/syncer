package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.config.pipeline.common.Connection;
import com.github.zzt93.syncer.config.pipeline.common.MysqlConnection;
import com.github.zzt93.syncer.config.pipeline.input.Schema;
import com.github.zzt93.syncer.consumer.InputSource;
import com.github.zzt93.syncer.producer.input.connect.BinlogInfo;
import com.github.zzt93.syncer.producer.input.connect.MasterConnector;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class LocalInputSource implements InputSource {

  private Logger logger = LoggerFactory.getLogger(MasterConnector.class);

  private final ConsumerRegistry consumerRegistry;

  private final List<Schema> schemas;
  private final MysqlConnection connection;
  private final BinlogInfo binlogInfo;
  private final String clientId;

  public LocalInputSource(
      ConsumerRegistry consumerRegistry,
      List<Schema> schemas,
      MysqlConnection connection,
      BinlogInfo binlogInfo, String clientId) {
    this.consumerRegistry = consumerRegistry;
    this.schemas = schemas;
    this.connection = connection;
    this.binlogInfo = binlogInfo;
    this.clientId = clientId;
  }

  @Override
  public boolean register() {
    return consumerRegistry.register(connection, this);
  }

  @Override
  public Connection getRemoteConnection() {
    return connection;
  }

  @Override
  public BinlogInfo getBinlogInfo() {
    return binlogInfo;
  }

  @Override
  public List<Schema> getSchemas() {
    return schemas;
  }

  @Override
  public String clientId() {
    return clientId;
  }

  @Override
  public boolean input(SyncData data) {
    return false;
  }

  @Override
  public boolean input(SyncData[] data) {
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LocalInputSource that = (LocalInputSource) o;

    return clientId.equals(that.clientId);
  }

  @Override
  public int hashCode() {
    return clientId.hashCode();
  }
}
