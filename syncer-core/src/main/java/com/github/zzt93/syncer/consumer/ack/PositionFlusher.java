package com.github.zzt93.syncer.consumer.ack;

import com.github.zzt93.syncer.common.thread.NotExitEventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class PositionFlusher implements NotExitEventLoop {

  private final Ack ack;
  private final Logger logger = LoggerFactory.getLogger(PositionFlusher.class);

  public PositionFlusher(Ack ack) {
    this.ack = ack;
  }

  @Override
  public void loop() {
    ack.flush();
  }
}
