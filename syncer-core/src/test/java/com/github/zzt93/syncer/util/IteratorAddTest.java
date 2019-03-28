package com.github.zzt93.syncer.util;


import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

public class IteratorAddTest {

  public void add() {
    List<Integer> dataList = new LinkedList<>();
    dataList.add(10);
    for (ListIterator<Integer> iterator = dataList.listIterator(); iterator.hasNext(); ) {
      Integer next = iterator.next();
      if (next == 10) {
        for (int i = 0; i < 4; i++) {
          iterator.add(i);
          iterator.previous();
        }
      }
      System.out.println(next);
    }
    System.out.println(dataList);
  }
}
