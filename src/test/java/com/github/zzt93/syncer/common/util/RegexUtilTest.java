package com.github.zzt93.syncer.common.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author zzt
 */
public class RegexUtilTest {

  @Test
  public void env() throws Exception {
    Pattern env = RegexUtil.env();
    Matcher m1 = env.matcher("        - name: \"menkor_${ACTIVE_PROFILE}.*\"");
    Assert.assertTrue(m1.find());
    String group0 = m1.group(0);
    String group1 = m1.group(1);
    Assert.assertEquals("", "${ACTIVE_PROFILE}", group0);
    Assert.assertEquals("", "ACTIVE_PROFILE", group1);
  }

  @Test
  public void getRegex() throws Exception {
    Pattern simple = RegexUtil.getRegex("simple");
    Assert.assertNull(simple);
    Pattern star = RegexUtil.getRegex("dev.*");
    Assert.assertNotNull(star);
  }

}