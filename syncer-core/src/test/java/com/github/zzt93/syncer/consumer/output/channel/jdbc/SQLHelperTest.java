package com.github.zzt93.syncer.consumer.output.channel.jdbc;

import com.github.zzt93.syncer.common.util.SQLHelper;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author zzt
 */
public class SQLHelperTest {

  @Test
  public void inSQL() {
    String s = "rt0s{4tioX^I39@nptPw-) ySA_'l]j]iro#}N].k8Zst2)(LF%1=JM3MvY=<T1&`[~(<8b}6;y)Zct0%`hsw`.h.POg@N9>\\')6KZY#8rpe4Iu;wBL-zW9*Ef.<kr)3jH{%&AK~a]";
    String inSQL = SQLHelper.inSQL(s);
    Assert.assertEquals("'rt0s{4tioX^I39@nptPw-) ySA_''l]j]iro#}N].k8Zst2)(LF%1=JM3MvY=<T1&`[~(<8b}6;y)Zct0%`hsw`.h.POg@N9>\\\\'')6KZY#8rpe4Iu;wBL-zW9*Ef.<kr)3jH{%&AK~a]'", inSQL);
  }
}