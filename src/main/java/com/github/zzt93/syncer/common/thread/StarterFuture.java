package com.github.zzt93.syncer.common.thread;

import com.github.zzt93.syncer.Starter;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author zzt
 */
public class StarterFuture {

  private final List<Future> futures;
  private final Starter starter;


  public StarterFuture(Starter starter, List<Future> futures) {
    this.futures = futures;
    this.starter = starter;
  }

  public void waitTermination() throws Exception {
    for (Future future : futures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        starter.close();
        return;
      }
    }
  }


}
