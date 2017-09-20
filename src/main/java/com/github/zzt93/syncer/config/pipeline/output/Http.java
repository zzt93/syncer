package com.github.zzt93.syncer.config.pipeline.output;

import com.github.zzt93.syncer.config.pipeline.common.HttpConnection;
import com.github.zzt93.syncer.output.HttpChannel;
import com.github.zzt93.syncer.output.OutputChannel;
import java.util.Collections;
import java.util.HashMap;

/**
 * @author zzt
 */
public class Http implements OutputChannelConfig {

  private HttpConnection connection;
  private HashMap<String, Object> jsonMapper = new HashMap<>();

  public Http() {
  }

  public HttpConnection getConnection() {
    return connection;
  }

  public void setConnection(HttpConnection connection) {
    this.connection = connection;
  }

  public HashMap<String, Object> getJsonMapper() {
    return jsonMapper;
  }

  public void setJsonMapper(HashMap<String, Object> jsonMapper) {
    this.jsonMapper = jsonMapper;
  }

  @Override
  public OutputChannel toChannel() {
    if (connection.valid()) {
      return new HttpChannel(connection, Collections.unmodifiableMap(jsonMapper));
    }
    throw new IllegalArgumentException();
  }
}
