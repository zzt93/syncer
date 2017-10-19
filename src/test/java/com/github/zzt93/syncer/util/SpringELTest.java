package com.github.zzt93.syncer.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.Test;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * Created by zzt on 10/19/17.
 *
 * <h3></h3>
 */
public class SpringELTest {

  private final SpelExpressionParser parser = new SpelExpressionParser();

  private static class Tmp {

    private int a;
    private String b;
    private Object o;

    public Tmp(int a, String b) {
      this.a = a;
      this.b = b;
    }

    public Tmp() {

    }

    public String getB() {
      return b;
    }

    public void setB(String b) {
      this.b = b;
    }

    public int getA() {
      return a;
    }

    public void setA(int a) {
      this.a = a;
    }

    public Object getO() {
      return o;
    }

    public void setO(Object obj) {
      o = obj;
    }
  }

  @Test
  public void template() {
    Tmp rootObject = new Tmp(1, "asd");
    StandardEvaluationContext context = new StandardEvaluationContext(rootObject);

    System.out.println(parser.parseExpression("#tmp = a").getValue(context, Integer.class));
    System.out.println(parser.parseExpression("b").getValue(context, String.class));
  }

  @Test
  public void selection() throws Exception {
    Iterable<Integer> it = () -> new Iterator<Integer>() {

      private int[] a = new int[]{1, 2, 3};
      private int index = 0;

      @Override
      public boolean hasNext() {
        return index < a.length;
      }

      @Override
      public Integer next() {
        return a[index++];
      }
    };
    Tmp tmp = new Tmp();
    tmp.setO(it);
    StandardEvaluationContext context = new StandardEvaluationContext(tmp);

    ArrayList<Integer> list = parser.parseExpression("o.?[true]").getValue(context,
        ArrayList.class);
    System.out.println(list);
  }
}
