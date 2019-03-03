package com.github.zzt93.syncer.config;

import com.google.common.base.CaseFormat;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author zzt
 */
public class CaseTest {

  @Test
  public void caseTest() throws Exception {
    String to = CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_CAMEL, "key-name");
    Assert.assertEquals("", "keyName", to);
    String keyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, "key_id");
    Assert.assertEquals("", "keyId", keyName);
  }
}