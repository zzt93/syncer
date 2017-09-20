package com.github.zzt93.syncer.output;

import static org.springframework.http.HttpMethod.POST;
import static org.springframework.http.HttpMethod.PUT;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.config.pipeline.common.HttpConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.ParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
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
  private final Map<String, Object> mapper;
  private final SpelExpressionParser parser;

  public HttpChannel(HttpConnection connection, Map<String, Object> jsonMapper) {
    this.restTemplate = new RestTemplate();
    this.connection = connection.toConnectionUrl(null);
    this.mapper = jsonMapper;
    this.parser = new SpelExpressionParser();
  }

  /**
   * <a href="https://stackoverflow.com/questions/22989500/is-resttemplate-thread-safe">
   * RestTemplate is thread safe</a>
   *
   * @param event the data from filter module
   */
  @Override
  public boolean output(SyncData event) {
    HashMap<String, Object> res = new HashMap<>();
    mapToRes(event, mapper, res);
    switch (event.getType()) {
      case UPDATE_ROWS:
        return execute(res, POST).is2xxSuccessful();
      case DELETE_ROWS:
        restTemplate.delete(connection);
        return true;
      case WRITE_ROWS:
        return execute(res, PUT).is2xxSuccessful();
    }
    return false;
  }

  private void mapToRes(SyncData event, Map<String, Object> mapper,
      HashMap<String, Object> res) {
    for (String key : mapper.keySet()) {
      Object o = mapper.get(key);
      if (o instanceof Map) {
        Map map = (Map) o;
        mapToRes(event, map, res);
      } else if (o instanceof String) {
        String expr = (String) o;
        // TODO 9/20/17 check expr contains template
        String value = parser.parseExpression(expr, ParserContext.TEMPLATE_EXPRESSION)
            .getValue(new StandardEvaluationContext(), String.class);
        Object data = event.getData(value);
        if (data == null) {
          res.put(key, value);
        } else {
          res.put(key, data);
        }
      }

    }
  }

  private HttpStatus execute(HashMap<String, Object> res, HttpMethod method) {
    return restTemplate.exchange(connection, method, new HttpEntity<Map>(res), Object.class)
        .getStatusCode();
  }

  @Override
  public boolean output(List<SyncData> batch) {
    return false;
  }

  @Override
  public String des() {
    return "HttpChannel{" +
        "connection='" + connection + '\'' +
        '}';
  }
}
