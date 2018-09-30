package com.github.zzt93.syncer;

import java.util.concurrent.atomic.AtomicBoolean;

public class ShutDownCenter {

  private static AtomicBoolean shutdown = new AtomicBoolean(false);

  public static void initShutDown() {
    boolean first = shutdown.compareAndSet(false, true);
    if (first) {
      System.exit(1);
    }
  }

  public static boolean inShutDown() {
    return shutdown.get();
  }

}
