package com.github.zzt93.syncer.util;

import com.github.zzt93.syncer.common.data.CommonTypeLocator;
import com.github.zzt93.syncer.common.data.SyncUtil;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.context.expression.MapAccessor;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * Created by zzt on 10/19/17.
 *
 * <h3></h3>
 */
public class SpringELTest {

  public static int a = 1;
  private final SpelExpressionParser parser = new SpelExpressionParser();

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

  @Test
  public void qualifier() throws Exception {
    StandardEvaluationContext context = new StandardEvaluationContext();
    context.setTypeLocator(typeName -> {
      try {
        return Class.forName("com.github.zzt93.syncer.util." + typeName);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException(e);
      }
    });
    Class testClass = parser.parseExpression("T(SpringELTest)").getValue(context, Class.class);
    Assert.assertEquals(testClass, SpringELTest.class);
    Integer value = parser.parseExpression("T(SpringELTest).a").getValue(context, Integer.class);
    Assert.assertTrue(value == 1);
  }

  @Test
  public void classVar() throws Exception {
    StandardEvaluationContext context = new StandardEvaluationContext();
    Class testClass = parser.parseExpression("T(String[])").getValue(context, Class.class);
    Assert.assertEquals(testClass, String[].class);
  }

  @Test
  public void typeLocator() throws Exception {
    StandardEvaluationContext context = new StandardEvaluationContext();
    context.setTypeLocator(new CommonTypeLocator());
    Class sync = parser.parseExpression("T(SyncUtil)").getValue(context, Class.class);
    Assert.assertEquals(sync, SyncUtil.class);
    Class map = parser.parseExpression("T(Map)").getValue(context, Class.class);
    Assert.assertEquals(map, Map.class);
  }

  @Test
  public void syncUtil() throws Exception {
    StandardEvaluationContext context = new StandardEvaluationContext();
    context.setTypeLocator(new CommonTypeLocator());
    context.setVariable("tags", "[\"AC\",\"BD\",\"CE\",\"DF\",\"GG\"]");
    String[] tags = parser.parseExpression("T(SyncUtil).fromJson(#tags, T(String[]))")
        .getValue(context, String[].class);
    Object tags2 = parser.parseExpression("T(SyncUtil).fromJson(#tags, T(String[]))")
        .getValue(context);
    Assert.assertEquals(tags.length, 5);
    Assert.assertEquals(tags2.getClass(), String[].class);
    Assert.assertEquals(((String[]) tags2).length, 5);
  }

  @Test
  public void projection() throws Exception {
    StandardEvaluationContext context = new StandardEvaluationContext();
    context.setTypeLocator(new CommonTypeLocator());
    context.addPropertyAccessor(new MapAccessor());
    context.setVariable("content",
        "{\"blocks\":[{\"data\":{},\"depth\":0,\"entityRanges\":[],\"inlineStyleRanges\":[],\"key\":\"ummxd\",\"text\":\"Test\",\"type\":\"unstyled\"}],\"entityMap\":{}}");
    String value = parser
        .parseExpression("T(SyncUtil).fromJson(#content,T(Map))['blocks'].![text]")
        .getValue(context, String.class);
    Assert.assertEquals(value, "Test");
  }

  @Test
  public void nullCheck() throws Exception {
    Boolean value = parser.parseExpression("null").getValue(Boolean.class);
    Assert.assertNull(value);
  }

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
}
