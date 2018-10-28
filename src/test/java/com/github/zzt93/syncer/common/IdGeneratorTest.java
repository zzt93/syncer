package com.github.zzt93.syncer.common;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author zzt
 */
public class IdGeneratorTest {

  @Test
  public void fromEventId() throws Exception {
    String asd = IdGenerator.fromEventId("asd", 10);
    Assert.assertEquals(asd, "asd/10");
  }


}