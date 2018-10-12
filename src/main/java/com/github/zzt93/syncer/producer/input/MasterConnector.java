package com.github.zzt93.syncer.producer.input;

import com.github.zzt93.syncer.common.thread.EventLoop;

/**
 * @author zzt
 */
public interface MasterConnector extends EventLoop {

  default void close() {
    logger.info("[Shutting down] {}", getClass().getSimpleName());
  }

}
