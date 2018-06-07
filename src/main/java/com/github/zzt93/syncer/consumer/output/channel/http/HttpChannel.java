package com.github.zzt93.syncer.consumer.output.channel.http;

import static org.springframework.http.HttpMethod.DELETE;
import static org.springframework.http.HttpMethod.POST;
import static org.springframework.http.HttpMethod.PUT;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.pipeline.common.HttpConnection;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;
import com.github.zzt93.syncer.consumer.output.mapper.KVMapper;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.RestTemplate;

/**
 * Should be thread safe
 *
 * @author zzt
 */
public class HttpChannel implements OutputChannel {

  private final Logger logger = LoggerFactory.getLogger(HttpChannel.class);
  private final RestTemplate restTemplate;
  private final String connection;
  private final KVMapper mapper;
  private final Ack ack;

  public HttpChannel(HttpConnection connection, Map<String, Object> jsonMapper,
      Ack ack) {
    this.ack = ack;
    this.restTemplate = new RestTemplate();
    this.connection = connection.toConnectionUrl(null);
    this.mapper = new KVMapper(jsonMapper);
  }

  /**
   * <a href="https://stackoverflow.com/questions/22989500/is-resttemplate-thread-safe">
   * RestTemplate is thread safe</a>
   *
   * @param event the data from filter module
   */
  @Override
  public boolean output(SyncData event) {
    // TODO 17/9/22 add batch worker
    // TODO 18/6/6 add ack
    HashMap<String, Object> res = mapper.map(event);
    logger.debug("Mapping table row {} to {}", event.getRecords(), res);
    switch (event.getType()) {
      case UPDATE_ROWS:
        return execute(res, POST).is2xxSuccessful();
      case DELETE_ROWS:
        return execute(res, DELETE).is2xxSuccessful();
      case WRITE_ROWS:
        return execute(res, PUT).is2xxSuccessful();
      default:
        return false;
    }
  }


  private HttpStatus execute(HashMap<String, Object> res, HttpMethod method) {
    return restTemplate.exchange(connection, method, new HttpEntity<Map>(res), Object.class)
        .getStatusCode();
  }

  @Override
  public String des() {
    return "HttpChannel{" +
        "connection='" + connection + '\'' +
        '}';
  }

}
