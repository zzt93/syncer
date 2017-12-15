package com.github.zzt93.syncer.config;

import java.io.IOException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

/**
 * @author zzt
 */
@Component
public class YamlEnvironmentPostProcessor implements EnvironmentPostProcessor {

  private final YamlPropertySourceLoader loader = new YamlPropertySourceLoader();

  @Override
  public void postProcessEnvironment(ConfigurableEnvironment environment,
      SpringApplication application) {
    String pipelineName = environment.getProperty("pipelineConfig");
    if (pipelineName == null) {
      throw new IllegalArgumentException("No pipeline config file specified, try '--pipelineConfig=sample.yml'");
    }
    String configFile = environment.getProperty("config");
    if (configFile == null) {
      configFile = "config.yml";
    }
    environment.getPropertySources().addLast(loadYaml(configFile));
    environment.getPropertySources().addLast(loadYaml(pipelineName));
  }

  private PropertySource<?> loadYaml(String name) {
    Resource path = new FileSystemResource(name);
    if (!path.exists()) {
      path = new ClassPathResource(name);
      if (!path.exists()) {
        throw new IllegalArgumentException(
            "Syncer config file is not on classpath and it is not a absolute path file, fail to find it: " + name);
      }
    }
    try {
      return this.loader.load(name, path, null);
    } catch (IOException ex) {
      throw new IllegalStateException(
          "Failed to load yaml configuration from " + path, ex);
    }
  }

}

