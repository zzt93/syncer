package com.github.zzt93.syncer.common.data;

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
  }

}