package com.github.zzt93.syncer.config.consumer.output;

import com.github.zzt93.syncer.config.ConsumerConfig;
import com.github.zzt93.syncer.config.consumer.output.elastic.Elasticsearch;
import com.github.zzt93.syncer.config.consumer.output.kafka.Kafka;
import com.github.zzt93.syncer.config.consumer.output.mysql.Mysql;
import com.github.zzt93.syncer.config.consumer.output.redis.Redis;
import com.github.zzt93.syncer.config.syncer.SyncerOutputMeta;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zzt
 */
@Getter
@Setter
@ConsumerConfig("output")
public class PipelineOutput {

  private Elasticsearch elasticsearch;
  private Mysql mysql;
  private Kafka kafka;
  private Redis redis;

  public List<OutputChannel> toOutputChannels(String consumerId, Ack ack,
                                              SyncerOutputMeta outputMeta)
      throws Exception {
    List<OutputChannel> res = new ArrayList<>();
    if (elasticsearch != null) {
      res.add(elasticsearch.toChannel(consumerId, ack, outputMeta));
    }
    if (mysql != null) {
      res.add(mysql.toChannel(consumerId, ack, outputMeta));
    }
    if (redis != null) {
      res.add(redis.toChannel(consumerId, ack, outputMeta));
    }
    if (kafka != null) {
      res.add(kafka.toChannel(consumerId, ack, outputMeta));
    }
    return res;
  }

  public int outputChannels() {
    int count = 0;
    if (elasticsearch != null) count++;
    if (mysql != null) count++;
    if (redis != null) count++;
    if (kafka != null) count++;
    return count;
  }
}
