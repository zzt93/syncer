package com.github.zzt93.syncer.consumer.output.channel.http;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.common.HttpConnection;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;
import com.github.zzt93.syncer.consumer.output.mapper.KVMapper;
import com.google.gson.Gson;
import io.netty.handler.codec.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final Logger logger = LoggerFactory.getLogger(HttpChannel.class);
  private final String connection;
  private final KVMapper mapper;
  private final Ack ack;
  private final String id;
  private final NettyClient nettyClient;

  public HttpChannel(HttpConnection connection, Map<String, Object> jsonMapper,
      Ack ack) {
    this.ack = ack;
    this.connection = connection.toConnectionUrl(null);
    nettyClient = new NettyClient(connection);
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
    // TODO 18/6/6 add ack
    HashMap<String, Object> res = mapper.map(event);
    logger.debug("Mapping table row {} to {}", event.getFields(), res);
    switch (event.getType()) {
      case UPDATE:
        return execute(res, POST);
      case DELETE:
        return execute(res, DELETE);
      case WRITE:
        return execute(res, PUT);
      default:
        return false;
    }
  }


  private boolean execute(HashMap<String, Object> res, HttpMethod method) throws InterruptedException {
    return nettyClient.write(method, gson.toJson(res));
  }

  @Override
  public String des() {
    return "HttpChannel{" +
        "connection='" + connection + '\'' +
        '}';
  }

  @Override
  public void close() {
    nettyClient.close();
  }

  @Override
  public String id() {
    return id;
  }

}
