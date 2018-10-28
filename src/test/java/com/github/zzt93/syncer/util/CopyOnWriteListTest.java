package com.github.zzt93.syncer.util;

import com.google.common.collect.Lists;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class CopyOnWriteListTest {

  private static final Logger logger = LoggerFactory.getLogger(CopyOnWriteListTest.class);
  private static final ExecutorService service = Executors.newFixedThreadPool(2);

  @Test
  public void removeNotVisitedInFor() throws Exception {
    CopyOnWriteArrayList<Integer> l = prepareList();
    int c = l.size();
    for (Integer integer : l) {
      if (integer == 2) {
        l.remove(2 + 1);
      }
      c--;
    }
    Assert.assertEquals(c, 0);
  }

  @Test
  public void removeVisitedInFor() throws Exception {
    CopyOnWriteArrayList<Integer> l = prepareList();
    int c = l.size();
    for (Integer integer : l) {
      if (integer == 2) {
        l.remove(2 - 1);
      }
      c--;
    }
    Assert.assertEquals(c, 0);
  }

  @Test
  public void removeCurrentInFor() throws Exception {
    CopyOnWriteArrayList<Integer> l = prepareList();
    int c = l.size();
    for (Integer integer : l) {
      if (integer == 2) {
        l.remove(2);
      }
      c--;
    }
    Assert.assertEquals(c, 0);
  }


  @Test
  public void removeThenFor() throws Exception {
    CopyOnWriteArrayList<Integer> l = prepareList();
    for (Integer integer : l) {
      if (integer == 2) {
        l.remove(2);
      }
    }
    int c = l.size();
    for (Integer integer : l) {
      c--;
    }
    Assert.assertEquals(c, 0);
  }

  @Test
  public void removeMultiThread() throws Exception {
    Random random = new Random();
    CopyOnWriteArrayList<Integer> l = prepareList();
    service.submit(() -> {
      while (true) {
        for (Integer integer : l) {
          logger.info("{}", integer);
          Thread.sleep(random.nextInt(5) * 100 + 200);
        }
      }
    });
    service.submit(() -> {
      while (true) {
        for (Integer integer : l) {
          logger.info("{}", integer);
          Thread.sleep(random.nextInt(5) * 100 + 100);
          if (integer == 2) {
            Integer remove = l.remove(2);
            logger.info("remove {}", remove);
          }
        }
      }
    });
    service.shutdown();
    service.awaitTermination(5, TimeUnit.SECONDS);
  }


  private CopyOnWriteArrayList<Integer> prepareList() {
    return new CopyOnWriteArrayList<>(Lists.newArrayList(0, 1, 2, 3, 4));
  }

}
