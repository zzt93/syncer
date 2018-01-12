package com.github.zzt93.syncer.config;

import com.github.zzt93.syncer.common.util.FileUtil;
import com.github.zzt93.syncer.config.pipeline.PipelineConfig;
import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;

/**
 * @author zzt
 */
@Component
public class YamlEnvironmentPostProcessor implements EnvironmentPostProcessor {

  private static final Logger logger = LoggerFactory.getLogger(YamlEnvironmentPostProcessor.class);
  private static final String PIPELINE_CONFIG = "pipelineConfig";
  private static final String CONFIG = "config";
  private final YamlPropertySourceLoader loader = new YamlPropertySourceLoader();
  private final ArrayList<PipelineConfig> configs = new ArrayList<>();

  @Override
  public void postProcessEnvironment(ConfigurableEnvironment environment,
      SpringApplication application) {
    String pipelineNames = environment.getProperty(PIPELINE_CONFIG);
    if (pipelineNames == null) {
      throw new IllegalArgumentException("No pipeline config file specified, try '--" + PIPELINE_CONFIG
          + "=sample.yml,sample2.yml'");
    }
    String configFile = environment.getProperty(CONFIG);
    if (configFile == null) {
      configFile = "config.yml";
    }
    environment.getPropertySources().addLast(loadYaml(configFile));
    for (String fileName : pipelineNames.split(",")) {
      configs.add(initPipelines(fileName));
    }
  }

  private PipelineConfig initPipelines(String fileName) {
    Resource path = FileUtil.getResource(fileName);
    Yaml yaml = new Yaml();
    try (InputStream in = path.getInputStream()) {
      // TODO 18/1/5 convert key to camel case & handle environment variable & add prefix (`pipeline:`)
//      CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_CAMEL, "key-name")
      return yaml.loadAs(in, PipelineConfig.class);
    } catch (IOException e) {
      logger.error("Fail to load/parse yml file", e);
      throw new InvalidConfigException(e);
    }
  }

  private PropertySource<?> loadYaml(String name) {
    Resource path = FileUtil.getResource(name);
    try {
      return this.loader.load(name, path, null);
    } catch (IOException ex) {
      throw new IllegalStateException(
          "Failed to load yaml configuration from " + path, ex);
    }
  }

  public ArrayList<PipelineConfig> getConfigs() {
    return configs;
  }
}

