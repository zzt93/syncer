package com.github.zzt93.syncer.common.thread;

import com.github.zzt93.syncer.common.exception.ShutDownException;
import com.google.common.base.Throwables;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public interface VoidCallable extends Callable<Void> {

  Logger logger = LoggerFactory.getLogger(VoidCallable.class);

  @Override
  default Void call() throws Exception {
    try {
      loop();
    } catch (ShutDownException e) {
      logger.info("Shutting down ...");
      throw e;
    } catch (Throwable e) {
      logger.error("Fail to loop: {}", getClass(), e);
      Throwables.throwIfUnchecked(e);
    }
    return null;
  }

  void loop();

}
