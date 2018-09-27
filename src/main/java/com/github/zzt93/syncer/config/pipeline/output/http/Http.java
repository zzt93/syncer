package com.github.zzt93.syncer.config.pipeline.output.http;


import com.github.zzt93.syncer.config.pipeline.common.HttpConnection;
import com.github.zzt93.syncer.config.pipeline.output.FailureLogConfig;
import com.github.zzt93.syncer.config.pipeline.output.OutputChannelConfig;
import com.github.zzt93.syncer.config.pipeline.output.PipelineBatchConfig;
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
  private PipelineBatchConfig batch = new PipelineBatchConfig();
  private FailureLogConfig failureLog = new FailureLogConfig();

  public FailureLogConfig getFailureLog() {
    return failureLog;
  }

  public void setFailureLog(FailureLogConfig failureLog) {
    this.failureLog = failureLog;
  }

  public Http() {
    // default value of json mapper
    jsonMapping.put(KVMapper.FAKE_KEY, KVMapper.ROW_FLATTEN);
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

  public PipelineBatchConfig getBatch() {
    return batch;
  }

  public void setBatch(PipelineBatchConfig batch) {
    this.batch = batch;
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
      return new HttpChannel(connection, Collections.unmodifiableMap(getJsonMapping()), ack);
    }
    throw new IllegalArgumentException();
  }
}
