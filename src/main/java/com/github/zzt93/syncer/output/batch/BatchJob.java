package com.github.zzt93.syncer.output.batch;

import com.github.zzt93.syncer.output.channel.BufferedChannel;
import com.github.zzt93.syncer.output.channel.OutputChannel;

/**
 * @author zzt
 */
public class BatchJob implements Runnable {


  private final BufferedChannel buffer;

  public BatchJob(BufferedChannel buffer) {
    this.buffer = buffer;
  }

  @Override
  public void run() {
      buffer.flush();
  }
}
