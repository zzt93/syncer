package com.github.zzt93.syncer.common.thread;

import com.github.zzt93.syncer.common.LogbackLoggingField;
import org.slf4j.MDC;

/**
 * @author zzt
 */
public abstract class MDCRunnable implements Runnable {

  @Override
  public final void run() {
    MDC.put(LogbackLoggingField.EID, eid());
    runBody();
    MDC.remove(LogbackLoggingField.EID);
  }

  public abstract void runBody();
  public abstract String eid();

}
