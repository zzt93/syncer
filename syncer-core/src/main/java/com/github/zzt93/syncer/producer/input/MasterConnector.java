package com.github.zzt93.syncer.producer.input;

import com.github.zzt93.syncer.common.thread.EventLoop;
import com.github.zzt93.syncer.producer.input.mysql.connect.ColdStart;

import java.util.List;

/**
 * @author zzt
 */
public interface MasterConnector extends EventLoop {

  default void close() {
    logger.info("[Shutting down] {}", getClass().getSimpleName());
  }

  default List<ColdStart> coldStart() {
    return null;
  }
}
