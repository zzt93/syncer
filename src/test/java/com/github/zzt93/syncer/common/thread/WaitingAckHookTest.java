package com.github.zzt93.syncer.common.thread;

import org.junit.Before;
import org.junit.Test;

public class WaitingAckHookTest {

  @Before
  public void setUp() throws Exception {
  }

  public void exit() {
    Runtime.getRuntime().addShutdownHook(new Thread(()-> System.out.println("exit")));
    System.exit(0);
  }

  public void exitMulti() {
    Runtime.getRuntime().addShutdownHook(new Thread(()-> {
      System.out.println("exit");
      // deadlock
//      System.exit(0);
//      System.out.println("exit2");
    }));
    System.exit(0);
  }
}