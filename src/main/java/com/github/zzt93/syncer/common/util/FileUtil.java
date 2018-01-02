package com.github.zzt93.syncer.common.util;

import java.io.IOException;
import java.io.InputStreamReader;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.util.FileCopyUtils;

/**
 * @author zzt
 */
public class FileUtil {

  public static String readAll(String resourceName) throws IOException {
    return FileCopyUtils
        .copyToString(new InputStreamReader(getResource(resourceName).getInputStream()));
  }

  public static Resource getResource(String fileName) {
    Resource path = new FileSystemResource(fileName);
    if (!path.exists()) {
      path = new ClassPathResource(fileName);
      if (!path.exists()) {
        throw new IllegalArgumentException(
            "Config file [" + fileName
                + "] is not on classpath and it is not a absolute path file, fail to find it");
      }
    }
    return path;
  }
}
