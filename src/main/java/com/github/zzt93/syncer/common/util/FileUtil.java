package com.github.zzt93.syncer.common.util;

import java.io.IOException;
import java.io.InputStreamReader;
import org.springframework.util.FileCopyUtils;

/**
 * @author zzt
 */
public class FileUtil {

  public static String readAll(String resourceName) throws IOException {
    return FileCopyUtils
        .copyToString(new InputStreamReader(ClassLoader.getSystemResourceAsStream(resourceName)));
  }
}
