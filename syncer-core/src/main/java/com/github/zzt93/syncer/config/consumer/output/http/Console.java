package com.github.zzt93.syncer.config.consumer.output.http;


import com.github.zzt93.syncer.common.util.SyncDataTypeUtil;
import com.github.zzt93.syncer.config.common.HttpConnection;
import com.github.zzt93.syncer.config.consumer.output.OutputChannelConfig;
import com.github.zzt93.syncer.config.syncer.SyncerOutputMeta;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.http.ConsoleChannel;
import com.github.zzt93.syncer.consumer.output.channel.mapper.KVMapper;

import java.util.Collections;
import java.util.LinkedHashMap;

/**
 * @author zzt
 */
public class Console implements OutputChannelConfig {

  private HttpConnection connection;
  private LinkedHashMap<String, Object> jsonMapping = new LinkedHashMap<>();
  private String path;

  public Console() {
    // default value of json mapper
    jsonMapping.put(KVMapper.FAKE_KEY, SyncDataTypeUtil.ROW_FLATTEN);
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
  public ConsoleChannel toChannel(String consumerId, Ack ack,
                                  SyncerOutputMeta outputMeta) {
    this.consumerId = consumerId;
    if (connection.valid()) {
      return new ConsoleChannel(this, Collections.unmodifiableMap(getJsonMapping()), ack);
    }
    throw new IllegalArgumentException();
  }
}
