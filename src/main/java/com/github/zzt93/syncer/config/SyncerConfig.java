package com.github.zzt93.syncer.config;

import com.github.zzt93.syncer.config.filter.Filter;
import com.github.zzt93.syncer.config.input.Input;
import com.github.zzt93.syncer.config.output.Output;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * @author zzt
 */
//@PropertySource("classpath:syncer.properties") // not work with yml file!
@Configuration
@ConfigurationProperties(prefix = "syncer")
public class SyncerConfig {

    private Input input;
    private Output output;
    private List<Filter> filter;

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

