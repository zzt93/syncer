package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.consumer.ack.Ack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class PositionFlusher implements Runnable {

  private final Ack ack;
  private final Logger logger = LoggerFactory.getLogger(PositionFlusher.class);

  PositionFlusher(Ack ack) {
    this.ack = ack;
  }

  @Override
  public void run() {
    try {
      ack.flush();
    } catch (Exception e) {
      logger.error("Fail to flush ack info", e);
    }
  }
}
