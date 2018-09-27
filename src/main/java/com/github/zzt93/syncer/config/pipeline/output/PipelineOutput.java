package com.github.zzt93.syncer.config.pipeline.output;

import com.github.zzt93.syncer.config.pipeline.output.elastic.Elasticsearch;
import com.github.zzt93.syncer.config.pipeline.output.http.Http;
import com.github.zzt93.syncer.config.pipeline.output.mysql.Mysql;
import com.github.zzt93.syncer.config.pipeline.output.redis.Redis;
import com.github.zzt93.syncer.config.syncer.SyncerOutputMeta;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zzt
 */
public class PipelineOutput {

  private Elasticsearch elasticsearch;
  private Http http;
  private Mysql mysql;
  private Redis redis;

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
    return res;
  }
}
