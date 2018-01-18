package com.github.zzt93.syncer.config.pipeline.output;

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
  private MySQL mysql;

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

  public MySQL getMysql() {
    return mysql;
  }

  public void setMysql(MySQL mysql) {
    this.mysql = mysql;
  }

  public List<OutputChannel> toOutputChannels(Ack ack) throws Exception {
    List<OutputChannel> res = new ArrayList<>();
    if (elasticsearch != null) {
      res.add(elasticsearch.toChannel(ack));
    }
    if (http != null) {
      res.add(http.toChannel(ack));
    }
    if (mysql != null) {
      res.add(mysql.toChannel(ack));
    }
    return res;
  }
}
