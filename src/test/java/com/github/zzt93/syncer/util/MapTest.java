package com.github.zzt93.syncer.util;

import org.junit.Test;

import java.util.HashMap;

public class MapTest {

  @Test
  public void compute() {
    HashMap<Integer, String> map = new HashMap<>();
    map.computeIfAbsent(1, k->"1");
    System.out.println(map.get(1));
    map.computeIfAbsent(1, k->"2");
    System.out.println(map.get(1));
  }
}
