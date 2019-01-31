package com.github.zzt93.syncer.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author zzt
 */
public class ArgUtil {

  private static final String DASH = "--";
  private static final Logger logger = LoggerFactory.getLogger(ArgUtil.class);

  public static HashMap<String, String> toMap(String[] args) {
    HashMap<String, String> argKV = new HashMap<>();
    for (String arg : args) {
      if (arg.startsWith(DASH)) {
        String[] split = arg.split("=");
        checkArgument(split.length == 2, "Invalid arg format: %s", arg);
        String dash = split[0].substring(0, 2);
        checkArgument(dash.equals(DASH) && split[0].length() > 2, "Invalid arg format: %s", arg);
        argKV.put(split[0].substring(2), split[1]);
      } else {
        logger.error("Unsupported arg: {}", arg);
      }
    }
    return argKV;
  }
}
