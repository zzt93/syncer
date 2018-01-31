package com.github.zzt93.syncer.config.pipeline.output;

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

  public List<OutputChannel> toOutputChannels(Ack ack, SyncerOutputMeta outputMeta)
      throws Exception {
    List<OutputChannel> res = new ArrayList<>();
    if (elasticsearch != null) {
      res.add(elasticsearch.toChannel(ack, outputMeta));
    }
    if (http != null) {
      res.add(http.toChannel(ack, outputMeta));
    }
    if (mysql != null) {
      res.add(mysql.toChannel(ack, outputMeta));
    }
    return res;
  }
}
