package com.github.zzt93.syncer.consumer.output.channel.http;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.common.HttpConnection;
import com.github.zzt93.syncer.config.consumer.output.http.Http;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;
import com.github.zzt93.syncer.consumer.output.mapper.KVMapper;
import com.google.gson.Gson;
import io.netty.handler.codec.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.util.HashMap;
import java.util.Map;

import static io.netty.handler.codec.http.HttpMethod.*;


/**
 * Should be thread safe
 *
 * @author zzt
 */
public class HttpChannel implements OutputChannel {

  private static final Gson gson = new Gson();
  private static final int MAX_TRY = 5;
  private final Logger logger = LoggerFactory.getLogger(HttpChannel.class);
  private final String connection;
  private final KVMapper mapper;
  private final Ack ack;
  private final String id;
  private final NettyHttpClient httpClient;
  private Expression pathExpr;

  public HttpChannel(Http http, Map<String, Object> jsonMapper, Ack ack) {
    HttpConnection connection = http.getConnection();
    if (http.getPath() != null) {
      SpelExpressionParser parser = new SpelExpressionParser();
      pathExpr = parser.parseExpression(http.getPath());
    }
    this.ack = ack;
    this.connection = connection.toConnectionUrl(null);
    httpClient = new NettyHttpClient(connection, new HttpClientInitializer());
    id = connection.connectionIdentifier();
    this.mapper = new KVMapper(jsonMapper);
  }

  /**
   * <a href="https://stackoverflow.com/questions/22989500/is-resttemplate-thread-safe">
   * RestTemplate is thread safe</a>
   *
   * @param event the data from filter module
   */
  @Override
  public boolean output(SyncData event) throws InterruptedException {
    // TODO 17/9/22 add batch worker
    HashMap<String, Object> map = mapper.map(event);
    String path = pathExpr.getValue(event.getContext(), String.class);

    boolean res = false;
    int count = 0;
    while (!res && count < MAX_TRY) {
      switch (event.getType()) {
        case UPDATE:
          res = execute(POST, path, map);
          break;
        case DELETE:
          res = execute(DELETE, path, map);
          break;
        case WRITE:
          res = execute(PUT, path, map);
          break;
        default:
          logger.error("Unsupported event type: {}", event);
          return false;
      }
      if (res) {
        ack.remove(event.getSourceIdentifier(), event.getDataId());
      } else {
        count++;
        if (count == MAX_TRY) {
          logger.error("Fail to send {}", event);
        }
      }
    }
    return false;
  }


  private boolean execute(HttpMethod method, String path, HashMap<String, Object> content) throws InterruptedException {
    return httpClient.write(method, path, gson.toJson(content));
  }

  @Override
  public String des() {
    return "HttpChannel{" +
        "connection='" + connection + '\'' +
        '}';
  }

  @Override
  public void close() {
    httpClient.close();
  }

  @Override
  public String id() {
    return id;
  }

}
