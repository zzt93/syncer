package com.github.zzt93.syncer.config.pipeline.output;


import com.github.zzt93.syncer.config.pipeline.common.HttpConnection;
import com.github.zzt93.syncer.output.channel.OutputChannel;
import com.github.zzt93.syncer.output.channel.http.HttpChannel;
import com.github.zzt93.syncer.output.mapper.JsonMapper;
import java.util.Collections;
import java.util.HashMap;

/**
 * @author zzt
 */
public class Http implements OutputChannelConfig {

  private HttpConnection connection;
  private HashMap<String, Object> jsonMapping = new HashMap<>();
  private PipelineBatch batch = new PipelineBatch();

  public Http() {
    // default value of json mapper
    jsonMapping.put(JsonMapper.FAKE_KEY, JsonMapper.ROW_FLATTEN);
  }

  public HttpConnection getConnection() {
    return connection;
  }

  public void setConnection(HttpConnection connection) {
    this.connection = connection;
  }

  public HashMap<String, Object> getJsonMapping() {
    if (jsonMapping.size() > 1 && jsonMapping.containsKey(JsonMapper.FAKE_KEY)) {
      jsonMapping.remove(JsonMapper.FAKE_KEY);
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
  public OutputChannel toChannel() {
    if (connection.valid()) {
      return new HttpChannel(connection, Collections.unmodifiableMap(getJsonMapping()));
    }
    throw new IllegalArgumentException();
  }
}
