package com.github.zzt93.syncer.config.pipeline.output;

import com.github.zzt93.syncer.output.channel.OutputChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zzt
 */
public class PipelineOutput {

  private Elasticsearch elasticsearch;
  private Http http;

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

  public List<OutputChannel> toOutputChannels() throws Exception {
    List<OutputChannel> res = new ArrayList<>();
    if (elasticsearch != null) {
      res.add(elasticsearch.toChannel());
    }
    if (http != null) {
      res.add(http.toChannel());
    }
    return res;
  }
}
