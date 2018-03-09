package com.github.zzt93.syncer.config.pipeline.common;

import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisPassword;

/**
 * @author zzt
 */
public class RedisClusterConnection extends ClusterConnection {

  public RedisClusterConfiguration getConfig() {
    RedisClusterConfiguration configuration = new RedisClusterConfiguration(
        getClusterNodes());
    configuration.setPassword(RedisPassword.of(getPassword()));
    return configuration;
  }

  @Override
  public boolean valid() {
    return getClusterNodes() != null && !getClusterNodes().isEmpty();
  }
}
