package com.github.zzt93.syncer.common.thread;

import com.github.zzt93.syncer.common.exception.ShutDownException;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This kind of loop should not exit unless user requested
 * @author zzt
 */
public interface NotExitEventLoop extends EventLoop {

  Logger logger = LoggerFactory.getLogger(NotExitEventLoop.class);

  @Override
  default void run() {
    boolean notExit = true;
    while (notExit) {
      try {
        loop();
        notExit = false;
      } catch (InvalidConfigException e) {
        logger.error("Invalid config", e);
      } catch (ShutDownException e) {
        logger.info("Shutting down ...");
        notExit = false;
      } catch (Throwable e) {
        logger.error("Fail to loop: {}", getClass());
      }
    }
  }

  void loop();

}
