package com.github.zzt93.syncer.consumer.output.channel.redis;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncWrapper;
import com.github.zzt93.syncer.config.pipeline.common.RedisConnection;
import com.github.zzt93.syncer.config.pipeline.output.FailureLogConfig;
import com.github.zzt93.syncer.config.pipeline.output.PipelineBatch;
import com.github.zzt93.syncer.config.pipeline.output.Redis;
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
import org.springframework.data.redis.core.RedisTemplate;

/**
 * @author zzt
 */
public class RedisChannel implements BufferedChannel {

  private final Logger logger = LoggerFactory.getLogger(RedisChannel.class);
  private final BatchBuffer<SyncWrapper> batchBuffer;
  private final PipelineBatch batch;
  private final Ack ack;
  private final FailureLog<SyncWrapper<String>> request;
  private final RedisTemplate<String, Object> template;


  public RedisChannel(Redis redis, SyncerOutputMeta outputMeta, Ack ack) {
    this.batch = redis.getBatch();
    batchBuffer = new BatchBuffer<>(batch, SyncWrapper.class);
    this.ack = ack;
    RedisConnection connection = redis.getConnection();
    FailureLogConfig failureLog = redis.getFailureLog();
    try {
      Path path = Paths.get(outputMeta.getFailureLogDir(), connection.connectionIdentifier());
      request = new FailureLog<>(path, failureLog, new TypeToken<SyncWrapper<String>>() {
      });
    } catch (FileNotFoundException e) {
      throw new IllegalStateException("Impossible", e);
    }
    template = new RedisTemplate<>();
    template.setConnectionFactory(new JedisConnectionFactory(connection.getConfig()));
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
      for (SyncWrapper wrapper : aim) {
      }
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

  }

  @Override
  public boolean output(SyncData event) {
    return false;
  }

  @Override
  public String des() {
    return "RedisChannel{" +
        "template=" + template +
        '}';
  }
}
