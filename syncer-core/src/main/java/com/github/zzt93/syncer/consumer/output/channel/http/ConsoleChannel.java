package com.github.zzt93.syncer.consumer.output.channel.http;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.consumer.output.http.Console;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Should be thread safe
 *
 * @author zzt
 */
public class ConsoleChannel implements OutputChannel {

  private final Logger logger = LoggerFactory.getLogger(ConsoleChannel.class);
  private final Ack ack;

  public ConsoleChannel(Console console, Ack ack) {
    this.ack = ack;
  }

  /**
   * @param event the data from filter module
   */
  @Override
  public boolean output(SyncData event) {
    ack.remove(event.getSourceIdentifier(), event.getDataId());
    logger.info("[Debug] {}", event);
    return true;
  }

  @Override
  public void close() {
  }

  @Override
  public String id() {
    return "console";
  }

}
