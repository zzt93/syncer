package com.github.zzt93.syncer.common.util;

import java.util.regex.Pattern;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author zzt
 */
public class RegexUtilTest {

  @Test
  public void getRegex() throws Exception {
    Pattern simple = RegexUtil.getRegex("simple");
    Assert.assertNull(simple);
    Pattern star = RegexUtil.getRegex("dev.*");
    Assert.assertNotNull(star);
  }

}