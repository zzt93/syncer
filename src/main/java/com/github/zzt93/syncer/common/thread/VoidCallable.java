package com.github.zzt93.syncer.common.thread;

import java.util.concurrent.Callable;

/**
 * @author zzt
 */
public interface VoidCallable extends Callable<Void> {

  @Override
  default Void call() throws Exception {
    loop();
    return null;
  }

  void loop();

}
