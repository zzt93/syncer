package com.github.zzt93.syncer.common.thread;

import com.github.zzt93.syncer.common.util.NamedThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author zzt
 */
public class ThreadPoolTest {


  public static void main(String[] args) {
//    normalExit();
//    exceptionExit();
    threadInterrupt();
  }

  private static void normalExit() {
    ExecutorService service = Executors
        .newFixedThreadPool(1, new NamedThreadFactory("syncer-producer"));
    service.submit(() -> System.out.println("asdf"));
  }

  private static void exceptionExit() {
    ExecutorService service = Executors
        .newFixedThreadPool(1, new NamedThreadFactory("syncer-producer"));
    service.submit(() -> {
      throw new IllegalStateException();
    });
  }

  private static void threadInterrupt() {
    int nThreads = 2;
    ExecutorService service = Executors
        .newFixedThreadPool(nThreads, new NamedThreadFactory("syncer-producer"));
    for (int i = 0; i < nThreads; i++) {
      service.submit((Runnable) () -> {
        while (true) {
          if (Thread.currentThread().isInterrupted()) {
            System.out.println("interrupted");
            break;
          }
        }
      });
    }
//    service.shutdown();
//    service.shutdownNow();
  }
}