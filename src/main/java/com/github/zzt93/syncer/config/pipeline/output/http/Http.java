package com.github.zzt93.syncer.config.pipeline.output.http;


import com.github.zzt93.syncer.config.pipeline.common.HttpConnection;
import com.github.zzt93.syncer.config.pipeline.output.FailureLogConfig;
import com.github.zzt93.syncer.config.pipeline.output.OutputChannelConfig;
import com.github.zzt93.syncer.config.pipeline.output.PipelineBatch;
import com.github.zzt93.syncer.config.syncer.SyncerOutputMeta;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.http.HttpChannel;
import com.github.zzt93.syncer.consumer.output.mapper.KVMapper;
import java.util.Collections;
import java.util.HashMap;

/**
 * @author zzt
 */
public class Http implements OutputChannelConfig {

  private HttpConnection connection;
  private HashMap<String, Object> jsonMapping = new HashMap<>();
  private PipelineBatch batch = new PipelineBatch();
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

  public HashMap<String, Object> getJsonMapping() {
    if (jsonMapping.size() > 1 && jsonMapping.containsKey(KVMapper.FAKE_KEY)) {
      jsonMapping.remove(KVMapper.FAKE_KEY);
    }
    return jsonMapping;
  }

  public void setJsonMapping(HashMap<String, Object> jsonMapping) {
    this.jsonMapping = jsonMapping;
  }

  public PipelineBatch getBatch() {
    return batch;
  }

  public void setBatch(PipelineBatch batch) {
    this.batch = batch;
  }

  @Override
  public HttpChannel toChannel(Ack ack,
      SyncerOutputMeta outputMeta) {
    if (connection.valid()) {
      return new HttpChannel(connection, Collections.unmodifiableMap(getJsonMapping()), ack);
    }
    throw new IllegalArgumentException();
  }
}
