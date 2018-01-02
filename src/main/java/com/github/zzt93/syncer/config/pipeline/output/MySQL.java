package com.github.zzt93.syncer.config.pipeline.output;

import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import com.github.zzt93.syncer.config.pipeline.common.MysqlConnection;
import com.github.zzt93.syncer.output.channel.OutputChannel;
import com.github.zzt93.syncer.output.channel.jdbc.MySQLChannel;

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

  public MySQL setConnection(
      MysqlConnection connection) {
    this.connection = connection;
    return this;
  }

  public PipelineBatch getBatch() {
    return batch;
  }

  public MySQL setBatch(PipelineBatch batch) {
    this.batch = batch;
    return this;
  }

  public RowMapping getRowMapping() {
    return rowMapping;
  }

  public MySQL setRowMapping(RowMapping rowMapping) {
    this.rowMapping = rowMapping;
    return this;
  }

  @Override
  public OutputChannel toChannel() throws Exception {
    if (connection.valid()) {
      return new MySQLChannel(connection, rowMapping, batch);
    }
    throw new InvalidConfigException("Invalid connection configuration: " + connection);
  }

}
