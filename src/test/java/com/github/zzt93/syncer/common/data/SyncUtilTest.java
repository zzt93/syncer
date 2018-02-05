package com.github.zzt93.syncer.common.data;

import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author zzt
 */
public class SyncUtilTest {

  @Test
  public void fromJson() throws Exception {
    Object o = SyncUtil.fromJson("[\"AC\",\"BD\",\"CE\",\"DF\",\"GG\"]", String[].class);
    Assert.assertEquals(o.getClass(), String[].class);
    String[] ss = (String[]) o;
    Assert.assertEquals(ss.length, 5);
    Object map = SyncUtil.fromJson("{a:1, b:\"asd\"}", Map.class);
    Map m = (Map) map;
    Assert.assertEquals(m.size(),2);
    Assert.assertEquals(m.get("a"),1.0);
    Assert.assertEquals(m.get("b"),"asd");
  }

}