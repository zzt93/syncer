package com.github.zzt93.syncer.common.util;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Random;

/**
 * @author zzt
 */
public class RandomDataUtil {
  private static final int MIN = 32;
  private static final int MAX = 127;
  private static Random r = new Random();
  private static final long EARLIEST = Timestamp.valueOf("1974-01-01 00:00:00").getTime();
  private static final long END = Timestamp.valueOf("2028-01-01 00:00:00").getTime();

  public static Timestamp randomTimestamp() {
    long diff = END - EARLIEST + 1;
    long l = (long) (Math.random() * diff);
    return new Timestamp(EARLIEST + l);
  }

  public static Date randomDate() {
    return new Date(randomTimestamp().getTime());
  }

  public static String random(int min, int max, boolean hasQuote) {
    int l = r.nextInt(max - min) + min;
    StringBuilder sb = new StringBuilder(l + 2);
    if (hasQuote) {
      sb.append("'");
    }
    for (int i = 0; i < l; i++) {
      char c = randomAscii();
      while (c == ',' || c == '\\' || (hasQuote && c == '\'')) {
        c = randomAscii();
      }
      sb.append(c);
    }
    if (hasQuote) {
      sb.append("'");
    }
    return sb.toString();
  }

  public static char randomAscii() {
    return (char) (MIN + r.nextInt(MAX - MIN));
  }

  public static BigDecimal randomDecimal() {
    return new BigDecimal(r.nextLong()).movePointLeft(2);
  }

  public static byte randomByte() {
    return (byte) r.nextInt(Byte.MAX_VALUE);
  }

  public static byte[] randomBytes(int len) {
    byte[] bytes = new byte[len];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = randomByte();
    }
    return bytes;
  }
}
