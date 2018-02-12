package com.github.zzt93.syncer.config.pipeline.output;

import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import com.github.zzt93.syncer.config.pipeline.common.RedisConnection;
import com.github.zzt93.syncer.config.syncer.SyncerOutputMeta;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.redis.RedisChannel;

/**
 * @author zzt
 */
public class Redis implements OutputChannelConfig {

  private RedisConnection connection;
  private RequestMapping requestMapping = new RequestMapping();
  private PipelineBatch batch = new PipelineBatch();
  private FailureLogConfig failureLog = new FailureLogConfig();

  public FailureLogConfig getFailureLog() {
    return failureLog;
  }

  public void setFailureLog(FailureLogConfig failureLog) {
    this.failureLog = failureLog;
  }

  public RedisConnection getConnection() {
    return connection;
  }

  public void setConnection(RedisConnection connection) {
    this.connection = connection;
  }

  public RequestMapping getRequestMapping() {
    return requestMapping;
  }

  public void setRequestMapping(RequestMapping requestMapping) {
    this.requestMapping = requestMapping;
  }


  public PipelineBatch getBatch() {
    return batch;
  }

  public void setBatch(PipelineBatch batch) {
    this.batch = batch;
  }

  @Override
  public RedisChannel toChannel(Ack ack,
      SyncerOutputMeta outputMeta) throws Exception {
    if (connection.valid()) {
      return new RedisChannel(this, outputMeta, ack);
    }
    throw new InvalidConfigException("Invalid connection configuration: " + connection);
  }
}
