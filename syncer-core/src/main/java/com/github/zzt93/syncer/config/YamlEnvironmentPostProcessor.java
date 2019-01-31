package com.github.zzt93.syncer.config;

import com.github.zzt93.syncer.SyncerApplication;
import com.github.zzt93.syncer.common.util.ArgUtil;
import com.github.zzt93.syncer.common.util.FileUtil;
import com.github.zzt93.syncer.common.util.RegexUtil;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.consumer.ConsumerConfig;
import com.github.zzt93.syncer.config.consumer.ProducerConfig;
import com.github.zzt93.syncer.config.syncer.SyncerConfig;
import com.github.zzt93.syncer.producer.register.LocalConsumerRegistry;
import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;

/**
 * @author zzt
 */
public class YamlEnvironmentPostProcessor {

  private static final Logger logger = LoggerFactory.getLogger(YamlEnvironmentPostProcessor.class);
  private static final String CONSUMER_CONFIG = "consumerConfig";
  private static final String PRODUCER_CONFIG = "producerConfig";
  private static final String CONFIG = "config";
  private static final String APPLICATION = "application.yml";

  public static SyncerApplication processEnvironment(String[] args) {
    HashMap<String, String> argKV = ArgUtil.toMap(args);

    String configFile = argKV.get(CONFIG);
    if (configFile == null) {
      configFile = "syncer.yml";
    }
    SyncerConfig syncerConfig = initConfig(configFile, SyncerConfig.class);

    String producer = argKV.get(PRODUCER_CONFIG);
    if (producer == null) {
      throw new IllegalArgumentException(
          "No producer config file specified, try '--" + PRODUCER_CONFIG
              + "=producer.yml");
    }
    ProducerConfig producerConfig = initConfig(producer, ProducerConfig.class);

    Set<String> filePaths = getFilePaths(argKV.get(CONSUMER_CONFIG));
    ArrayList<ConsumerConfig> configs = new ArrayList<>();
    for (String fileName : filePaths) {
      configs.add(initConfig(fileName, ConsumerConfig.class));
    }

    String version = getVersion();
    return new SyncerApplication(producerConfig, syncerConfig, new LocalConsumerRegistry(), configs, version);
  }

  private static String getVersion() {
    String str;
    try {
      str = FileUtil.readAll(FileUtil.getResource(APPLICATION).getInputStream());
    } catch (IOException e) {
      logger.error("Fail to load/parse {} file", APPLICATION, e);
      throw new InvalidConfigException(e);
    }
    Yaml yaml = new Yaml();
    Map map = yaml.loadAs(str, Map.class);
    return map.get("syncer.version").toString();
  }

  private static Set<String> getFilePaths(String pipelineNames) {
    if (pipelineNames == null) {
      throw new IllegalArgumentException(
          "No consumer config file specified, try '--" + CONSUMER_CONFIG
              + "=sample.yml,sample2.yml'");
    }
    Set<String> res = new HashSet<>();
    for (String name : pipelineNames.split(",")) {
      Path path = Paths.get(name);
      if (Files.isDirectory(path)) {
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(path, "*.{yml,yaml}")) {
          paths.iterator().forEachRemaining(p -> res.add(p.toString()));
        } catch (IOException e) {
          logger.error("Fail to travel {}", path, e);
          throw new InvalidConfigException(e);
        }
      } else {
        res.add(name);
      }
    }
    return res;
  }

  private static <T> T initConfig(String fileName, Class<T> tClass) {
    Resource path = FileUtil.getResource(fileName);
    Yaml yaml = getYaml(tClass);
    try (InputStream in = path.getInputStream()) {
      String str = replaceEnv(in);
      return yaml.loadAs(str, tClass);
    } catch (IOException e) {
      logger.error("Fail to load/parse {} as {}", fileName, tClass, e);
      throw new InvalidConfigException(e);
    }
  }

  private static <T> Yaml getYaml(Class<T> tClass) {
    Constructor c = new Constructor(tClass);
    c.setPropertyUtils(new PropertyUtils() {
      @Override
      public Property getProperty(Class<?> type, String name) {
        if (name.indexOf('-') > -1) {
          name = CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_CAMEL, name);
        }
        return super.getProperty(type, name);
      }
    });
    return new Yaml(c);
  }

  private static String replaceEnv(InputStream in)
      throws IOException {
    String str = FileUtil.readAll(in);
    Matcher matcher = RegexUtil.env().matcher(str);
    HashMap<String, String> rep = new HashMap<>();
    while (matcher.find()) {
      String group = matcher.group(1);
      String property = System.getenv(group);
      Preconditions.checkNotNull(property, "Fail to resolve env var: %s", group);
      rep.put(matcher.group(), property);
    }
    for (Entry<String, String> entry : rep.entrySet()) {
      str = str.replace(entry.getKey(), entry.getValue());
    }
    return str;
  }

}

