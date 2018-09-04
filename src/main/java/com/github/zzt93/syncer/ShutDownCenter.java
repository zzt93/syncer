package com.github.zzt93.syncer;

import java.util.concurrent.atomic.AtomicBoolean;

public class ShutDownCenter {

  private static AtomicBoolean shutdown = new AtomicBoolean(false);

  public static boolean initShutDown() {
    boolean b = shutdown.compareAndSet(false, true);
    if (b) {
      System.exit(1);
    }
    return b;
  }

  public static boolean inShutDown() {
    return shutdown.get();
  }

}
