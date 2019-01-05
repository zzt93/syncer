package com.github.zzt93.syncer.config.consumer.output.mysql;

import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.common.MysqlConnection;
import com.github.zzt93.syncer.config.consumer.output.BufferedOutputChannelConfig;
import com.github.zzt93.syncer.config.syncer.SyncerOutputMeta;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;
import com.github.zzt93.syncer.consumer.output.channel.jdbc.MysqlChannel;

/**
 * @author zzt
 */
public class Mysql extends BufferedOutputChannelConfig {

  private MysqlConnection connection;
  private RowMapping rowMapping;

  public MysqlConnection getConnection() {
    return connection;
  }

  public void setConnection(
      MysqlConnection connection) {
    this.connection = connection;
  }

  public RowMapping getRowMapping() {
    return rowMapping;
  }

  public void setRowMapping(RowMapping rowMapping) {
    this.rowMapping = rowMapping;
  }

  private String consumerId;

  @Override
  public String getConsumerId() {
    return consumerId;
  }

  @Override
  public OutputChannel toChannel(String consumerId, Ack ack, SyncerOutputMeta outputMeta) throws Exception {
    this.consumerId = consumerId;
    if (connection.valid()) {
      return new MysqlChannel(this, outputMeta, ack);
    }
    throw new InvalidConfigException("Invalid connection configuration: " + connection);
  }

}
