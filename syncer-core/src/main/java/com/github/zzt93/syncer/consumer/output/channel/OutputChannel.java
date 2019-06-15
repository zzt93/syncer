package com.github.zzt93.syncer.consumer.output.channel;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.github.zzt93.syncer.consumer.output.channel.elastic.ElasticsearchChannel;
import com.github.zzt93.syncer.consumer.output.channel.http.HttpChannel;
import com.github.zzt93.syncer.consumer.output.channel.jdbc.MysqlChannel;
import com.github.zzt93.syncer.consumer.output.channel.kafka.KafkaChannel;
import com.github.zzt93.syncer.consumer.output.channel.redis.RedisChannel;

/**
 * @author zzt
 */
public interface OutputChannel {

  /**
   * <ul>
   * <li>Should be thread safe; Should retry if failed</li>
   * <li>Should not be async, otherwise may in disorder</li>
   * </ul>
   *
   * @param event the data from filter module
   * @return whether output is success
   * @see HttpChannel is async, so depracated
   * @see ElasticsearchChannel is sync
   * @see MysqlChannel is sync
   * @see KafkaChannel is async, but the order is ensured by kafka client
   * @see RedisChannel not implemented for the time being
   */
  @ThreadSafe
  boolean output(SyncData event) throws InterruptedException;

  String des();

  void close();

  String id();
}
