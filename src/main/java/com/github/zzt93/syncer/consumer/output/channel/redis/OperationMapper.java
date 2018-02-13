package com.github.zzt93.syncer.consumer.output.channel.redis;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import com.github.zzt93.syncer.config.pipeline.output.redis.OperationMapping;
import com.github.zzt93.syncer.consumer.output.mapper.Mapper;
import java.io.UnsupportedEncodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.expression.Expression;
import org.springframework.expression.ParseException;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * @author zzt
 */
public class OperationMapper implements Mapper<SyncData, RedisCallback> {

  private final Logger logger = LoggerFactory.getLogger(OperationMapper.class);
  private final OperationMapping mapping;
  private final SpelExpressionParser parser;
  private Expression keyExpr;
  private Expression valueExpr;

  public OperationMapper(OperationMapping mapping) {
    this.mapping = mapping;
    parser = new SpelExpressionParser();
    try {
      keyExpr = parser.parseExpression(mapping.getKey());
      valueExpr = parser.parseExpression(mapping.getValue());
    } catch (ParseException | IllegalStateException e) {
      throw new InvalidConfigException("Fail to parse [mapping] config for [redis] output", e);
    }
  }

  @Override
  public RedisCallback map(SyncData data) {
    StandardEvaluationContext context = data.getContext();
    String key = keyExpr.getValue(context, String.class);
    Object value = valueExpr.getValue(context);
    final byte[] rawKey = rawKey(key);
    final byte[] rawValue = rawValue(value);

    return connection -> {
      mapping.operationMethod(connection).apply(rawKey, rawValue);
      return null;
    };
  }

  private byte[] rawKey(String key) {
    try {
      return key.getBytes("utf8");
    } catch (UnsupportedEncodingException ignored) {
      logger.error("Impossible", ignored);
      throw new IllegalStateException();
    }
  }
  private byte[] rawValue(Object value) {
    try {
      return ((String) value).getBytes("utf8");
    } catch (UnsupportedEncodingException ignored) {
      logger.error("Impossible", ignored);
      throw new IllegalStateException();
    }
  }
}
