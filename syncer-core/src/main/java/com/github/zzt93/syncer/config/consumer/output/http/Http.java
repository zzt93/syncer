package com.github.zzt93.syncer.config.consumer.output.http;


import com.github.zzt93.syncer.config.common.HttpConnection;
import com.github.zzt93.syncer.config.consumer.output.OutputChannelConfig;
import com.github.zzt93.syncer.config.syncer.SyncerOutputMeta;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.http.HttpChannel;
import com.github.zzt93.syncer.consumer.output.mapper.KVMapper;

import java.util.Collections;
import java.util.LinkedHashMap;

/**
 * @author zzt
 */
public class Http implements OutputChannelConfig {

  private HttpConnection connection;
  private LinkedHashMap<String, Object> jsonMapping = new LinkedHashMap<>();
  private String path;

  public Http() {
    // default value of json mapper
    jsonMapping.put(KVMapper.FAKE_KEY, KVMapper.ROW_FLATTEN);
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public HttpConnection getConnection() {
    return connection;
  }

  public void setConnection(HttpConnection connection) {
    this.connection = connection;
  }

  public LinkedHashMap<String, Object> getJsonMapping() {
    if (jsonMapping.size() > 1 && jsonMapping.containsKey(KVMapper.FAKE_KEY)) {
      jsonMapping.remove(KVMapper.FAKE_KEY);
    }
    return jsonMapping;
  }

  public void setJsonMapping(LinkedHashMap<String, Object> jsonMapping) {
    this.jsonMapping = jsonMapping;
  }

  private String consumerId;

  @Override
  public String getConsumerId() {
    return consumerId;
  }

  @Override
  public HttpChannel toChannel(String consumerId, Ack ack,
      SyncerOutputMeta outputMeta) {
    this.consumerId = consumerId;
    if (connection.valid()) {
      return new HttpChannel(this, Collections.unmodifiableMap(getJsonMapping()), ack);
    }
    throw new IllegalArgumentException();
  }
}
