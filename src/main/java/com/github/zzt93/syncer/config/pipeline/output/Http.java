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
  private HashMap<String, Object> jsonMapper = new HashMap<>();
  private PipelineBatch batch = new PipelineBatch();

  public Http() {
    // default value of json mapper
    jsonMapper.put(JsonMapper.FAKE_KEY, JsonMapper.ROW_FLATTEN);
  }

  public HttpConnection getConnection() {
    return connection;
  }

  public void setConnection(HttpConnection connection) {
    this.connection = connection;
  }

  public HashMap<String, Object> getJsonMapper() {
    if (jsonMapper.size() > 1 && jsonMapper.containsKey(JsonMapper.FAKE_KEY)) {
      jsonMapper.remove(JsonMapper.FAKE_KEY);
    }
    return jsonMapper;
  }

  public void setJsonMapper(HashMap<String, Object> jsonMapper) {
    this.jsonMapper = jsonMapper;
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
      return new HttpChannel(connection, Collections.unmodifiableMap(getJsonMapper()));
    }
    throw new IllegalArgumentException();
  }
}
