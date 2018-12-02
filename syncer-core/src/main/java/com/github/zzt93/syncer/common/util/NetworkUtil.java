package com.github.zzt93.syncer.common.util;

import com.google.common.base.Preconditions;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author zzt
 */
public class NetworkUtil {

  public static String toIp(String host) throws UnknownHostException {
    InetAddress address = InetAddress.getByName(host);
    return address.getHostAddress();
  }

  public static long ipToLong(String ip) {
    long res = 0;
    Preconditions.checkState(ip.split("\\.").length == 4);
    for (String s : ip.split("\\.")) {
      res = (res << 8) + Short.parseShort(s);
    }
    return res;
  }
}
