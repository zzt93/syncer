package com.github.zzt93.syncer.consumer.output.channel.http;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.common.HttpConnection;
import com.github.zzt93.syncer.config.consumer.output.http.Console;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


/**
 * Should be thread safe
 *
 * @author zzt
 */
public class ConsoleChannel implements OutputChannel {

  private final Logger logger = LoggerFactory.getLogger(ConsoleChannel.class);
  private final Ack ack;
  private final String id;

  public ConsoleChannel(Console console, Map<String, Object> jsonMapper, Ack ack) {
    HttpConnection connection = console.getConnection();
    this.ack = ack;
    id = connection.connectionIdentifier();
  }

  /**
   * @param event the data from filter module
   */
  @Override
  public boolean output(SyncData event) {
    ack.remove(event.getSourceIdentifier(), event.getDataId());
    logger.info("{}", event);
    return true;
  }

  @Override
  public String des() {
    return "ConsoleChannel";
  }

  @Override
  public void close() {
  }

  @Override
  public String id() {
    return id;
  }

}
