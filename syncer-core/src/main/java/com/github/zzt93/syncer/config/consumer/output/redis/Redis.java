package com.github.zzt93.syncer.config.consumer.output.redis;

import com.github.zzt93.syncer.config.common.Connection;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.common.RedisClusterConnection;
import com.github.zzt93.syncer.config.consumer.output.BufferedOutputChannelConfig;
import com.github.zzt93.syncer.config.syncer.SyncerOutputMeta;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.redis.RedisChannel;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

/**
 * @author zzt
 */
public class Redis extends BufferedOutputChannelConfig {

  private RedisClusterConnection clusterConnection = new RedisClusterConnection();
  private Connection connection = new Connection();
  private OperationMapping mapping = new OperationMapping();
  private String condition;

  public RedisClusterConnection getClusterConnection() {
    return clusterConnection;
  }

  public void setClusterConnection(RedisClusterConnection clusterConnection) {
    this.clusterConnection = clusterConnection;
  }

  public OperationMapping getMapping() {
    return mapping;
  }

  public void setMapping(OperationMapping mapping) {
    this.mapping = mapping;
  }

  public void setCondition(String condition) {
    this.condition = condition;
  }

  public Connection getConnection() {
    return connection;
  }

  public void setConnection(Connection connection) {
    this.connection = connection;
  }

  private String consumerId;

  @Override
  public String getConsumerId() {
    return consumerId;
  }

  @Override
  public RedisChannel toChannel(String consumerId, Ack ack,
      SyncerOutputMeta outputMeta) throws Exception {
    this.consumerId = consumerId;
    if (clusterConnection.valid() || connection.valid()) {
      return new RedisChannel(this, outputMeta, ack);
    }
    throw new InvalidConfigException("Invalid connection configuration: " + clusterConnection);
  }

  @Override
  public String conditionExpr() {
    return condition;
  }

  public LettuceConnectionFactory getConnectionFactory() {
    if (clusterConnection.valid()) {
      return new LettuceConnectionFactory(clusterConnection.getConfig());
    }
    if (connection.valid()) {
      RedisStandaloneConfiguration standaloneConfig = new RedisStandaloneConfiguration(
          connection.getAddress(),
          connection.getPort());
      if (!connection.noPassword()) {
        standaloneConfig.setPassword(RedisPassword.of(connection.getPassword()));
      }
      return new LettuceConnectionFactory(standaloneConfig);
    }
    throw new InvalidConfigException("No valid connection configured");
  }

  public String connectionIdentifier() {
    if (clusterConnection.valid()) {
      return clusterConnection.connectionIdentifier();
    } else if (connection.valid()) {
      return connection.initIdentifier();
    }
    throw new InvalidConfigException("No valid connection configured");
  }
}
