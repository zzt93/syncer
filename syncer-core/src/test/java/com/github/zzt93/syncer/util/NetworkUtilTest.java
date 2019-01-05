package com.github.zzt93.syncer.util;

import static org.junit.Assert.assertEquals;

import com.github.zzt93.syncer.common.util.NetworkUtil;
import org.junit.Test;

/**
 * @author zzt
 */
public class NetworkUtilTest {

  @Test
  public void toIp() throws Exception {
    String host = "192.168.1.100";
    String s = NetworkUtil.toIp(host);
    assertEquals(s, host);
  }

  @Test
  public void ipToInt() {
    String h1 = "0.0.0.1";
    long l1 = NetworkUtil.ipToLong(h1);
    assertEquals(l1, 0x1L);
    String h2 = "192.168.1.100";
    long l2 = NetworkUtil.ipToLong(h2);
    assertEquals(l2, 0xc0a80164L);
  }

}