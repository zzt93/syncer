package com.github.zzt93.syncer.common.expr;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author zzt
 */
public class ParameterReplace {

  private static final Pattern PARAMETER_PLACEHOLDER = Pattern.compile("\\?(\\d+)");

  public static String orderedParam(String input, String... var) {
    Matcher matcher = PARAMETER_PLACEHOLDER.matcher(input);
    String result = input;
    while (matcher.find()) {
      String group = matcher.group();
      int index = Integer.parseInt(matcher.group(1));
      result = result.replace(group, var[index]);
    }
    return result;
  }
}
