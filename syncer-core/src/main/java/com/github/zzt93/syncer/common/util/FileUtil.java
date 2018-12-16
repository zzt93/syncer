package com.github.zzt93.syncer.common.util;

import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.syncer.SyncerInputMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.util.FileCopyUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author zzt
 */
public class FileUtil {
  private static Logger logger = LoggerFactory.getLogger(SyncerInputMeta.class);

  public static String readAll(String resourceName) throws IOException {
    return FileCopyUtils
        .copyToString(new InputStreamReader(getResource(resourceName).getInputStream()));
  }

  public static List<String> readLine(String resourceName) throws IOException {
    return Files.readAllLines(getResource(resourceName).getFile().toPath());
  }

  public static String readAll(InputStream inputStream) throws IOException {
    return FileCopyUtils
        .copyToString(new InputStreamReader(inputStream));
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

  public static void createDirIfNotExist(String fullPath) {
    Path metaDir = Paths.get(fullPath);
    if (!Files.exists(metaDir)) {
      logger.info("path[{}] not exists, creating a new one", fullPath);
      try {
        Files.createDirectories(metaDir);
      } catch (IOException e) {
        logger.error("Fail to create dir, aborting", e);
        throw new InvalidConfigException(e);
      }
    }
  }

  public static void createFile(Path path, Consumer<IOException> consumer) {
    if (!Files.exists(path)) {
      try {
        createDirIfNotExist(path.getParent().toString());
        Files.createFile(path);
      } catch (IOException e) {
        consumer.accept(e);
      }
    }
  }
}
