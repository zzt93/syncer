package com.github.zzt93.syncer.util;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * @author zzt
 */
public class RegexUtil {

  public static Pattern getRegex(String input) {
    try {
      return Pattern.compile(input);
    } catch (PatternSyntaxException e) {
      return null;
    }
  }
}
