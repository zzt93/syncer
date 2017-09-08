package com.github.zzt93.syncer.config;

import com.github.zzt93.syncer.config.input.Input;
import com.github.zzt93.syncer.config.output.Output;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.io.ClassPathResource;

/**
 * @author zzt
 */
//@PropertySource("classpath:syncer.properties") // not work with yml file!
@Configuration
@ConfigurationProperties(prefix = "syncer")
public class SyncerConfig {

    private Input input;
    private Output output;

    /**
     * <h3>Time line:</h3>
     * <ul>
     * <li>{@link org.springframework.boot.autoconfigure.condition.ConditionalOnProperty ConditionalOnProperty}
     * resolve</li>
     * <li>This method: parse yml file, add to place holder configure</li>
     * <li>Init this class according to parsed config: {@link SyncerConfig}</li>
     * </ul>
     * <h3>Notice</h3>
     * This method have to be static, or impossible to set {@link SyncerConfig itself}
     */
//        @Bean
    public static PropertySourcesPlaceholderConfigurer properties(ConfigurableEnvironment environment) {
        PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer = new PropertySourcesPlaceholderConfigurer();
        YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new ClassPathResource("syncer.yml"));
        propertySourcesPlaceholderConfigurer.setProperties(yaml.getObject());
//        environment.getPropertySources().addLast(new PropertiesPropertySource("syncer", yaml.getObject()));
        return propertySourcesPlaceholderConfigurer;
    }

    public Input getInput() {
        return input;
    }

    public void setInput(Input input) {
        this.input = input;
    }

    public Output getOutput() {
        return output;
    }

    public void setOutput(Output output) {
        this.output = output;
    }


}

