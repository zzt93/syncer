package com.github.zzt93.syncer.config.consumer.output;

import com.github.zzt93.syncer.config.ConsumerConfig;
import com.github.zzt93.syncer.config.consumer.output.elastic.Elasticsearch;
import com.github.zzt93.syncer.config.consumer.output.http.Http;
import com.github.zzt93.syncer.config.consumer.output.kafka.Kafka;
import com.github.zzt93.syncer.config.consumer.output.mysql.Mysql;
import com.github.zzt93.syncer.config.consumer.output.redis.Redis;
import com.github.zzt93.syncer.config.syncer.SyncerOutputMeta;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zzt
 */
@ConsumerConfig("output")
public class PipelineOutput {

  private Elasticsearch elasticsearch;
  private Http http;
  private Mysql mysql;
  private Redis redis;
  private Kafka kafka;

  public Redis getRedis() {
    return redis;
  }

  public void setRedis(Redis redis) {
    this.redis = redis;
  }

  public Elasticsearch getElasticsearch() {
    return elasticsearch;
  }

  public void setElasticsearch(Elasticsearch elasticsearch) {
    this.elasticsearch = elasticsearch;
  }

  public Http getHttp() {
    return http;
  }

  public void setHttp(Http http) {
    this.http = http;
  }

  public Mysql getMysql() {
    return mysql;
  }

  public void setMysql(Mysql mysql) {
    this.mysql = mysql;
  }

  public Kafka getKafka() {
    return kafka;
  }

  public void setKafka(Kafka kafka) {
    this.kafka = kafka;
  }

  public List<OutputChannel> toOutputChannels(String consumerId, Ack ack,
                                              SyncerOutputMeta outputMeta)
      throws Exception {
    List<OutputChannel> res = new ArrayList<>();
    if (elasticsearch != null) {
      res.add(elasticsearch.toChannel(consumerId, ack, outputMeta));
    }
    if (http != null) {
      res.add(http.toChannel(consumerId, ack, outputMeta));
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
    if (http != null) count++;
    if (mysql != null) count++;
    if (redis != null) count++;
    if (kafka != null) count++;
    return count;
  }
}
