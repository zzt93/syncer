package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.config.pipeline.common.MysqlConnection;
import com.github.zzt93.syncer.config.pipeline.input.Schema;
import com.github.zzt93.syncer.consumer.InputSource;
import com.github.zzt93.syncer.producer.input.connect.BinlogInfo;
import com.github.zzt93.syncer.producer.input.connect.MasterConnector;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class LocalInputSource implements InputSource {

  private Logger logger = LoggerFactory.getLogger(MasterConnector.class);

  private ConsumerRegistry consumerRegistry;

  private List<Schema> schemas;
  private MysqlConnection connection;
  private BinlogInfo binlogInfo;
  private String clientId;

  public LocalInputSource(List<Schema> schemas) throws IOException {
    this.schemas = schemas;
  }

  @Override
  public boolean register() {
    return consumerRegistry.register(connection, this);
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
    return null;
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
