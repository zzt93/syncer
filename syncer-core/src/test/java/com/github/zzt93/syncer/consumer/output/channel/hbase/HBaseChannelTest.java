package com.github.zzt93.syncer.consumer.output.channel.hbase;

import com.github.zzt93.syncer.config.common.InvalidConfigException;
import org.apache.hadoop.hbase.util.FutureUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

public class HBaseChannelTest {

  public static void main(String[] args) throws InterruptedException {
    int size = 2;
    CountDownLatch run = new CountDownLatch(size);
    for (int i = 0; i < size; i++) {
      int finalI = i;
      CompletableFuture<Void> runAsync = CompletableFuture.runAsync(() -> {
        if (finalI % 2 == 0) {
          System.out.println("state0: run");
        } else {
          System.out.println("state0: throw");
          throw new InvalidConfigException();
        }
      });
      FutureUtils.addListener(runAsync, (r, e) -> {
        if (e != null) {
          System.out.println("state1: ex");
          runAsync.completeExceptionally(e);
        } else {
          System.out.println("state1: normal");
          runAsync.complete(null);
        }
      });
      runAsync.whenComplete((v, t) -> {
        if (t == null) {
          System.out.println("state2: handle");
        } else {
          System.out.println("state2: ex");
        }
        run.countDown();
      });
    }
    run.await();
  }

}