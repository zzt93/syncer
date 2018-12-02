package com.github.zzt93.syncer.common.thread;

public class WaitingAckHookTest {

  public static void main(String[] args) {

  }

  public void exit() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> System.out.println("exit")));
    System.exit(0);
  }

  public void exitMulti() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("exit");
      // deadlock
//      System.exit(0);
//      System.out.println("exit2");
    }));
    System.exit(0);
  }
}