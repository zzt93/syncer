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
//    System.out.println(Long.toString(NetworkUtil.ipToLong(h1), Character.MAX_RADIX) + ":" + 1234);
    String h2 = "192.168.1.100";
    long l2 = NetworkUtil.ipToLong(h2);
    assertEquals(l2, 0xc0a80164L);
//    System.out.println(Long.toString(NetworkUtil.ipToLong(h2), Character.MAX_RADIX) + ":" + Integer.toString(1234, Character.MAX_RADIX));
  }

  @Test
  public void strToIp() {
    String a1 = "1hge15w:ya";
    StringBuilder res = new StringBuilder();
    String[] split = a1.split(":");
    res.append(Long.parseLong(split[0], Character.MAX_RADIX)).append(":");
    res.append(Long.parseLong(split[1], Character.MAX_RADIX));
    System.out.println(res);
  }

}