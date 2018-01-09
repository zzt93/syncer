package com.github.zzt93.syncer.config.pipeline.output;

import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import com.github.zzt93.syncer.config.pipeline.common.MysqlConnection;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;
import com.github.zzt93.syncer.consumer.output.channel.jdbc.MySQLChannel;

/**
 * @author zzt
 */
public class MySQL implements OutputChannelConfig {

  private MysqlConnection connection;
  private RowMapping rowMapping;
  private PipelineBatch batch = new PipelineBatch();

  public MysqlConnection getConnection() {
    return connection;
  }

  public void setConnection(
      MysqlConnection connection) {
    this.connection = connection;
  }

  public PipelineBatch getBatch() {
    return batch;
  }

  public void setBatch(PipelineBatch batch) {
    this.batch = batch;
  }

  public RowMapping getRowMapping() {
    return rowMapping;
  }

  public void setRowMapping(RowMapping rowMapping) {
    this.rowMapping = rowMapping;
  }

  @Override
  public OutputChannel toChannel() throws Exception {
    if (connection.valid()) {
      return new MySQLChannel(connection, rowMapping, batch);
    }
    throw new InvalidConfigException("Invalid connection configuration: " + connection);
  }

}
