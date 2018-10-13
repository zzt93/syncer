package com.github.zzt93.syncer;

import com.github.zzt93.syncer.common.exception.ShutDownException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShutDownCenter {

  public static final int SHUTDOWN_TIMEOUT = 5;
  public static final int SHUTDOWN_MAX_TRY = 30;
  private static AtomicBoolean shutdown = new AtomicBoolean(false);
  private static final Logger logger = LoggerFactory.getLogger(ShutDownCenter.class);

  public static void initShutDown(Throwable e) {
    boolean first = shutdown.compareAndSet(false, true);
    if (first) {
      logger.error("[Shutting down] Init", e);
      System.exit(1);
    } else {
      logger.warn("[Shutting down]", e);
      throw new ShutDownException(e);
    }
  }

  public static void inShutdown() {
    shutdown.set(true);
  }

}
