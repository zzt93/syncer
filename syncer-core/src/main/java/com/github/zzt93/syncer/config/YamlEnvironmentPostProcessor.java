package com.github.zzt93.syncer.config;

import com.github.zzt93.syncer.common.util.FileUtil;
import com.github.zzt93.syncer.common.util.RegexUtil;
import com.github.zzt93.syncer.config.consumer.ConsumerConfig;
import com.github.zzt93.syncer.config.consumer.common.InvalidConfigException;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.Resource;
import org.yaml.snakeyaml.Yaml;

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
public class YamlEnvironmentPostProcessor implements EnvironmentPostProcessor {

  private static final Logger logger = LoggerFactory.getLogger(YamlEnvironmentPostProcessor.class);
  private static final String CONSUMER_CONFIG = "consumerConfig";
  private static final String PRODUCER_CONFIG = "producerConfig";
  private static final String CONFIG = "config";
  private static final ArrayList<ConsumerConfig> configs = new ArrayList<>();
  private final YamlPropertySourceLoader loader = new YamlPropertySourceLoader();

  @Override
  public void postProcessEnvironment(ConfigurableEnvironment environment,
      SpringApplication application) {
    String producer = environment.getProperty(PRODUCER_CONFIG);
    if (producer == null) {
      throw new IllegalArgumentException(
          "No producer config file specified, try '--" + PRODUCER_CONFIG
              + "=producer.yml");
    }
    Set<String> filePaths = getFilePaths(environment);
    String configFile = environment.getProperty(CONFIG);
    if (configFile == null) {
      configFile = "config.yml";
    }
    environment.getPropertySources().addLast(loadYaml(producer));
    environment.getPropertySources().addLast(loadYaml(configFile));
    for (String fileName : filePaths) {
      configs.add(initPipelines(fileName, environment));
    }
  }

  private Set<String> getFilePaths(ConfigurableEnvironment environment) {
    String pipelineNames = environment.getProperty(CONSUMER_CONFIG);
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
          paths.iterator().forEachRemaining(p->res.add(p.toString()));
        } catch (IOException e) {
          logger.error("Fail to load/parse yml file", e);
        }
      } else {
        res.add(name);
      }
    }
    return res;
  }

  private ConsumerConfig initPipelines(String fileName,
                                       ConfigurableEnvironment environment) {
    Resource path = FileUtil.getResource(fileName);
    Yaml yaml = new Yaml();
    try (InputStream in = path.getInputStream()) {
      // TODO 18/1/5 convert key to camel case & add prefix (`pipeline:`)
//      CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_CAMEL, "key-name")
      String str = replaceEnv(environment, in);
      return yaml.loadAs(str, ConsumerConfig.class);
    } catch (IOException e) {
      logger.error("Fail to load/parse yml file", e);
      throw new InvalidConfigException(e);
    }
  }

  private String replaceEnv(ConfigurableEnvironment environment, InputStream in)
      throws IOException {
    String str = FileUtil.readAll(in);
    Matcher matcher = RegexUtil.env().matcher(str);
    HashMap<String, String> rep = new HashMap<>();
    while (matcher.find()) {
      String group = matcher.group(1);
      String property = environment.getProperty(group);
      Preconditions.checkNotNull(property, "Fail to resolve env var: %s", group);
      rep.put(matcher.group(), property);
    }
    for (Entry<String, String> entry : rep.entrySet()) {
      str = str.replace(entry.getKey(), entry.getValue());
    }
    return str;
  }

  private PropertySource<?> loadYaml(String name) {
    Resource path = FileUtil.getResource(name);
    try {
      List<PropertySource<?>> load = this.loader.load(name, path);
      if (load.size() != 1) {
        throw new InvalidConfigException(
            "Invalid yaml configuration file " + path);
      }
      return load.get(0);
    } catch (IOException ex) {
      throw new IllegalStateException(
          "Failed to load yaml configuration from " + path, ex);
    }
  }

  public static ArrayList<ConsumerConfig> getConfigs() {
    return configs;
  }
}

