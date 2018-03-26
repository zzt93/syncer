package com.github.zzt93.syncer.common.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class NamedThreadFactory implements ThreadFactory {

  private static final AtomicLong all = new AtomicLong(1);
  private final AtomicLong every = new AtomicLong(1);
  private static Logger logger = LoggerFactory.getLogger(NamedThreadFactory.class);
  private final String prefix;

  public NamedThreadFactory() {
    prefix = "syncer-tmp";
  }

  public NamedThreadFactory(String prefix) {
    this.prefix = prefix;
  }

  @Override
  public Thread newThread(Runnable r) {
    String name = prefix + "-" + every.getAndAdd(1L) + "-" + all.getAndAdd(1L);
    logger.debug("Create a new thread: {}", name);
    return new Thread(r, name);
  }
}
