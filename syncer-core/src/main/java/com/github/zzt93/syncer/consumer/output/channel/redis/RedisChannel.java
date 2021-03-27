package com.github.zzt93.syncer.consumer.output.channel.redis;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.consumer.output.FailureLogConfig;
import com.github.zzt93.syncer.config.consumer.output.PipelineBatchConfig;
import com.github.zzt93.syncer.config.consumer.output.redis.Redis;
import com.github.zzt93.syncer.config.syncer.SyncerOutputMeta;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.batch.BatchBuffer;
import com.github.zzt93.syncer.consumer.output.channel.BufferedChannel;
import com.github.zzt93.syncer.consumer.output.channel.SyncWrapper;
import com.github.zzt93.syncer.consumer.output.failure.FailureEntry;
import com.github.zzt93.syncer.consumer.output.failure.FailureLog;
import com.google.gson.reflect.TypeToken;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.expression.Expression;
import org.springframework.expression.ParseException;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.StringUtils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * @author zzt
 */
@Getter
public class RedisChannel implements BufferedChannel<RedisCallback> {

  private final Logger logger = LoggerFactory.getLogger(RedisChannel.class);
  private final BatchBuffer<SyncWrapper<RedisCallback>> batchBuffer;
  private final PipelineBatchConfig batchConfig;
  private final Ack ack;
  private final FailureLog<SyncData> request;
  private final RedisTemplate<String, Object> template;
  private final OperationMapper operationMapper;
  private final String id;
  private Expression expression;


  public RedisChannel(Redis redis, SyncerOutputMeta outputMeta, Ack ack) {
    this.batchConfig = redis.getBatch();
    id = redis.connectionIdentifier();
    this.batchBuffer = new BatchBuffer<>(batchConfig);
    this.ack = ack;
    FailureLogConfig failureLog = redis.getFailureLog();
    Path path = Paths.get(outputMeta.getFailureLogDir(), id);
    request = FailureLog.getLogger(path, failureLog, new TypeToken<FailureEntry<SyncWrapper<String>>>() {
    });
    template = new RedisTemplate<>();
    LettuceConnectionFactory factory = redis.getConnectionFactory();
    factory.afterPropertiesSet();
    template.setConnectionFactory(factory);
    template.afterPropertiesSet();

    operationMapper = new OperationMapper(redis.getMapping());
    SpelExpressionParser parser = new SpelExpressionParser();
    if (!StringUtils.isEmpty(redis.conditionExpr())) {
      try {
        expression = parser.parseExpression(redis.conditionExpr());
      } catch (ParseException e) {
        throw new InvalidConfigException("Fail to parse [condition] for [redis] output channel", e);
      }
    }
  }

  @Override
  public void batchAndRetry(List<SyncWrapper<RedisCallback>> aim) throws InterruptedException {
    if (aim != null) {
      try {
        template.executePipelined((RedisCallback<Void>) connection -> {
          for (SyncWrapper<RedisCallback> wrapper : aim) {
            wrapper.getData().doInRedis(connection);
          }
          return null;
        });
        ackSuccess(aim);
      } catch (Exception e) {
        retryFailed(aim, e);
      }
    }
  }


  @Override
  public void retryFailed(List<SyncWrapper<RedisCallback>> aim, Throwable e) {
    for (SyncWrapper wrapper : aim) {
      wrapper.inc();
      if (wrapper.retryCount() > batchConfig.getMaxRetry()) {
        request.log(wrapper.getEvent(), e.getMessage());
      }
    }
    logger.error("{}", aim, e);
  }

  @Override
  public boolean output(SyncData event) {
    throw new UnsupportedOperationException("Not implemented");
    // TODO 18/4/16 add flushIfReachSizeLimit
//    if (expression == null) {
//      return batchBuffer.add(new SyncWrapper<>(event, operationMapper.map(event)));
//    }
//    Boolean value = expression.getValue(event.getContext(), Boolean.class);
//    if (value == null || !value) {
//      ack.remove(event.getSourceIdentifier(), event.getDataId());
//      return false;
//    }
//    return batchBuffer.add(new SyncWrapper<>(event, operationMapper.map(event)));
//    BufferedChannel.super.flushAndSetFlushDone(true);
  }

  @Override
  public void close() {

  }

  @Override
  public String id() {
    return id;
  }

}
