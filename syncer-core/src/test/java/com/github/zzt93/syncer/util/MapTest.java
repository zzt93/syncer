package com.github.zzt93.syncer.util;

import java.util.HashMap;
import org.junit.Assert;
import org.junit.Test;

public class MapTest {

  @Test
  public void compute() {
    HashMap<Integer, String> map = new HashMap<>();
    map.computeIfAbsent(1, k->"1");
    Assert.assertEquals(map.get(1), "1");
    map.computeIfAbsent(1, k->"2");
    Assert.assertEquals(map.get(1), "1");
  }
}
