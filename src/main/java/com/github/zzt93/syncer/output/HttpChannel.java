package com.github.zzt93.syncer.output;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.config.pipeline.common.HttpConnection;
import java.util.HashMap;
import java.util.List;
import org.springframework.web.client.RestTemplate;

/**
 * @author zzt
 */
public class HttpChannel implements OutputChannel {

  private final RestTemplate restTemplate;
  private final String connection;
  private final HashMap<String, Object> mapper;

  public HttpChannel(HttpConnection connection, HashMap<String, Object> jsonMapper) {
    this.restTemplate = new RestTemplate();
    this.connection = connection.toConnectionUrl(null);
    this.mapper = jsonMapper;
  }

  @Override
  public boolean output(SyncData event) {
    HashMap<String, Object> res = new HashMap<>();
    switch (event.getType()) {
      case UPDATE_ROWS:
        restTemplate.postForObject(connection, res, Boolean.class);
      case DELETE_ROWS:
        restTemplate.delete(connection);
        return true;
      case WRITE_ROWS:
        restTemplate.put(connection, res);
    }
    return false;
  }

  @Override
  public boolean output(List<SyncData> batch) {
    return false;
  }
}
