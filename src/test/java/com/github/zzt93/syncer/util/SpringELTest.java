package com.github.zzt93.syncer.util;

import org.junit.Test;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * Created by zzt on 10/19/17.
 *
 * <h3></h3>
 */
public class SpringELTest {

  private static class Tmp {

    private int a;
    private String b;

    public Tmp(int a, String b) {
      this.a = a;
      this.b = b;
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
  }

  @Test
  public void template() {
    SpelExpressionParser parser = new SpelExpressionParser();
    Tmp rootObject = new Tmp(1, "asd");
    StandardEvaluationContext context = new StandardEvaluationContext(rootObject);

    System.out.println(parser.parseExpression("#tmp = a").getValue(context, Integer.class));
    System.out.println(parser.parseExpression("b").getValue(context, String.class));
  }
}
