package com.github.zzt93.syncer.common.thread;

import com.github.zzt93.syncer.ShutDownCenter;
import com.github.zzt93.syncer.common.exception.ShutDownException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public interface EventLoop extends Runnable {

  Logger logger = LoggerFactory.getLogger(EventLoop.class);

  @Override
  default void run() {
    try {
      loop();
    } catch (ShutDownException e) {
      logger.info("Shutting down ...");
      throw e;
    } catch (Throwable e) {
      logger.error("Fail to loop: {}, init shut down", getClass(), e);
      ShutDownCenter.initShutDown();
    }
  }

  void loop();

}
