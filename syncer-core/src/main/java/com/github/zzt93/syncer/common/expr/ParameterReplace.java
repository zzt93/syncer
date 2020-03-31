package com.github.zzt93.syncer.common.expr;

import java.util.Arrays;

/**
 * @author zzt
 */
public class ParameterReplace {

  public static String orderedParam(String input, String... var) {
    StringBuilder sb = new StringBuilder(input.length() + Arrays.stream(var).mapToInt(String::length).sum());
    char[] cs = input.toCharArray();
    for (int i = 0; i < cs.length; i++) {
      if (cs[i] == '?') {
        // only support 0-9
        sb.append(var[cs[++i] - '0']);
      } else {
        sb.append(cs[i]);
      }
    }
    return sb.toString();
  }
}
