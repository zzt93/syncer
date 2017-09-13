package com.github.zzt93.syncer.input.connect;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class NamedThreadFactory implements ThreadFactory {

  private static final AtomicLong count = new AtomicLong(1);
  private static Logger logger = LoggerFactory.getLogger(NamedThreadFactory.class);

  @Override
  public Thread newThread(Runnable r) {
    return new Thread(r, "syncer-tmp-" + count.getAndAdd(1L));
  }
}
