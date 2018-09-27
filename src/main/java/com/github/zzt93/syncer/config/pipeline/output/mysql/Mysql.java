package com.github.zzt93.syncer.config.pipeline.output.mysql;

import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import com.github.zzt93.syncer.config.pipeline.common.MysqlConnection;
import com.github.zzt93.syncer.config.pipeline.output.FailureLogConfig;
import com.github.zzt93.syncer.config.pipeline.output.OutputChannelConfig;
import com.github.zzt93.syncer.config.pipeline.output.PipelineBatchConfig;
import com.github.zzt93.syncer.config.syncer.SyncerOutputMeta;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;
import com.github.zzt93.syncer.consumer.output.channel.jdbc.MysqlChannel;

/**
 * @author zzt
 */
public class Mysql implements OutputChannelConfig {

  private MysqlConnection connection;
  private RowMapping rowMapping;
  private PipelineBatchConfig batch = new PipelineBatchConfig();
  private FailureLogConfig failureLog = new FailureLogConfig();

  public FailureLogConfig getFailureLog() {
    return failureLog;
  }

  public void setFailureLog(FailureLogConfig failureLog) {
    this.failureLog = failureLog;
  }

  public MysqlConnection getConnection() {
    return connection;
  }

  public void setConnection(
      MysqlConnection connection) {
    this.connection = connection;
  }

  public PipelineBatchConfig getBatch() {
    return batch;
  }

  public void setBatch(PipelineBatchConfig batch) {
    this.batch = batch;
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
