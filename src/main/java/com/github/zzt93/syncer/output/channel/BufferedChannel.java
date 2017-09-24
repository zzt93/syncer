package com.github.zzt93.syncer.output.channel;

import java.util.concurrent.TimeUnit;

/**
 * Created by zzt on 9/24/17.
 *
 * <h3></h3>
 */
public interface BufferedChannel extends OutputChannel {

  long getDelay();

  TimeUnit getDelayUnit();

  void flush();
}
