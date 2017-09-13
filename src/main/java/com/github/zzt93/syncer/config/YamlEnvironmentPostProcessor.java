package com.github.zzt93.syncer.config;

import java.io.IOException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;
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
    Resource path = new ClassPathResource("syncer.yml");
    PropertySource<?> propertySource = loadYaml(path);
    environment.getPropertySources().addLast(propertySource);
  }

  private PropertySource<?> loadYaml(Resource path) {
    if (!path.exists()) {
      throw new IllegalArgumentException(
          "Sycner config file (syncer.yml) is not on classpath" + path);
    }
    try {
      return this.loader.load("custom-resource", path, null);
    } catch (IOException ex) {
      throw new IllegalStateException(
          "Failed to load yaml configuration from " + path, ex);
    }
  }

}

