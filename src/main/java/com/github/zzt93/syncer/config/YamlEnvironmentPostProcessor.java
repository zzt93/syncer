package com.github.zzt93.syncer.config;

import com.github.zzt93.syncer.common.util.FileUtil;
import java.io.IOException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

/**
 * @author zzt
 */
@Component
public class YamlEnvironmentPostProcessor implements EnvironmentPostProcessor {

  private static final String PIPELINE_CONFIG = "pipelineConfig";
  private static final String CONFIG = "config";
  private final YamlPropertySourceLoader loader = new YamlPropertySourceLoader();

  @Override
  public void postProcessEnvironment(ConfigurableEnvironment environment,
      SpringApplication application) {
    String pipelineName = environment.getProperty(PIPELINE_CONFIG);
    if (pipelineName == null) {
      throw new IllegalArgumentException("No pipeline config file specified, try '--" + PIPELINE_CONFIG
          + "=sample.yml'");
    }
    String configFile = environment.getProperty(CONFIG);
    if (configFile == null) {
      configFile = "config.yml";
    }
    environment.getPropertySources().addLast(loadYaml(configFile));
    environment.getPropertySources().addLast(loadYaml(pipelineName));
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

}

