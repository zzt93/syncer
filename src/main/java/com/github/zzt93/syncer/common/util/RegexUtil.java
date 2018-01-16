package com.github.zzt93.syncer.common.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * @author zzt
 */
public class RegexUtil {

  private static Pattern pattern = Pattern.compile("[.*+?^]");
  private static Pattern env = Pattern.compile("\\$\\{([^}]+)}");

  public static Pattern getRegex(String input) {
    Matcher matcher = pattern.matcher(input);
    if (!matcher.find()) {
      return null;
    }
    try {
      return Pattern.compile(input);
    } catch (PatternSyntaxException e) {
      return null;
    }
  }

  public static Pattern env() {
    return env;
  }
}
