package com.github.zzt93.syncer.config.pipeline.output.redis;

import java.util.function.BiFunction;
import org.springframework.data.redis.connection.RedisConnection;

/**
 * @author zzt
 */
public enum ValueType {
  STRING {
    @Override
    public BiFunction<byte[], byte[], Object> biFunction(
        RedisConnection connection, Operation operation) {
      switch (operation) {
        case set:
          return (k, v) -> {
            connection.set(k, v);
            return null;
          };
        case delete:
          return (k, v) -> connection.del(k);
        default:
          throw new UnsupportedOperationException();
      }
    }
  }, LIST {
    @Override
    public BiFunction<byte[], byte[], Object> biFunction(
        RedisConnection connection, Operation operation) {
      return null;
    }
  }, SET {
    @Override
    public BiFunction<byte[], byte[], Object> biFunction(
        RedisConnection connection, Operation operation) {
      return null;
    }
  }, HASH {
    @Override
    public BiFunction<byte[], byte[], Object> biFunction(
        RedisConnection connection, Operation operation) {
      return null;
    }
  }, ZSET {
    @Override
    public BiFunction<byte[], byte[], Object> biFunction(
        RedisConnection connection, Operation operation) {
      return null;
    }
  };

  public abstract BiFunction<byte[], byte[], Object> biFunction(
      RedisConnection connection, Operation operation);


  enum Operation {
    delete, set
  }
}
