package com.github.zzt93.syncer.consumer.output.channel.redis;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncWrapper;
import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import com.github.zzt93.syncer.config.pipeline.output.FailureLogConfig;
import com.github.zzt93.syncer.config.pipeline.output.PipelineBatch;
import com.github.zzt93.syncer.config.pipeline.output.redis.Redis;
import com.github.zzt93.syncer.config.syncer.SyncerOutputMeta;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.ack.FailureLog;
import com.github.zzt93.syncer.consumer.output.batch.BatchBuffer;
import com.github.zzt93.syncer.consumer.output.channel.BufferedChannel;
import com.google.gson.reflect.TypeToken;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.expression.Expression;
import org.springframework.expression.ParseException;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.StringUtils;

/**
 * @author zzt
 */
public class RedisChannel implements BufferedChannel {

  private final Logger logger = LoggerFactory.getLogger(RedisChannel.class);
  private final BatchBuffer<SyncWrapper> batchBuffer;
  private final PipelineBatch batch;
  private final Ack ack;
  private final FailureLog<SyncData> request;
  private final RedisTemplate<String, Object> template;
  private final OperationMapper operationMapper;
  private Expression expression;


  public RedisChannel(Redis redis, SyncerOutputMeta outputMeta, Ack ack) {
    this.batch = redis.getBatch();
    batchBuffer = new BatchBuffer<>(batch, SyncWrapper.class);
    this.ack = ack;
    FailureLogConfig failureLog = redis.getFailureLog();
    try {
      Path path = Paths.get(outputMeta.getFailureLogDir(), redis.connectionIdentifier());
      request = new FailureLog<>(path, failureLog, new TypeToken<SyncWrapper<String>>() {
      });
    } catch (FileNotFoundException e) {
      throw new IllegalStateException("Impossible", e);
    }
    template = new RedisTemplate<>();
    JedisConnectionFactory factory = redis.getConnectionFactory();
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
  public long getDelay() {
    return batch.getDelay();
  }

  @Override
  public TimeUnit getDelayUnit() {
    return batch.getDelayTimeUnit();
  }

  @Override
  public void flush() {
    SyncWrapper[] aim = batchBuffer.flush();
    try {
      send(aim);
      ackSuccess(aim);
    } catch (Exception e) {
      retryFailed(aim, e);
    }
  }

  private void send(SyncWrapper[] aim) {
    if (aim != null && aim.length != 0) {
      template.executePipelined((RedisCallback<Void>) connection -> {
        for (SyncWrapper<RedisCallback> wrapper : aim) {
          wrapper.getData().doInRedis(connection);
        }
        return null;
      });
    }
  }

  @Override
  public void flushIfReachSizeLimit() {
    SyncWrapper[] wrappers = batchBuffer.flushIfReachSizeLimit();
    send(wrappers);
  }

  @Override
  public void ackSuccess(SyncWrapper[] wrappers) {
    for (SyncWrapper wrapper : wrappers) {
      ack.remove(wrapper.getSourceId(), wrapper.getSyncDataId());
    }
  }

  @Override
  public void retryFailed(SyncWrapper[] wrappers, Exception e) {
    for (SyncWrapper wrapper : wrappers) {
      wrapper.inc();
      if (wrapper.retryCount() > batch.getMaxRetry()) {
        request.log(wrapper.getEvent());
      }
    }
    logger.error("{}", wrappers, e);
  }

  @Override
  public boolean retriable(Exception e) {
    return true;
  }

  @Override
  public boolean output(SyncData event) {
    if (expression == null) {
      return batchBuffer.add(new SyncWrapper<>(event, operationMapper.map(event)));
    }
    Boolean value = expression.getValue(event.getContext(), Boolean.class);
    if (value == null || !value) {
      ack.remove(event.getSourceIdentifier(), event.getDataId());
      return false;
    }
    return batchBuffer.add(new SyncWrapper<>(event, operationMapper.map(event)));
  }

  @Override
  public String des() {
    return "RedisChannel{" +
        "template=" + template +
        '}';
  }
}
