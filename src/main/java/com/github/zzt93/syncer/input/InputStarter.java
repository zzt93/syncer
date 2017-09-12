package com.github.zzt93.syncer.input;

import com.github.zzt93.syncer.config.input.Input;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author zzt
 */
public class InputStarter {

    private Logger logger = LoggerFactory.getLogger(InputStarter.class);
    private Input inputConfig;

    public InputStarter(Input inputConfig) {
        this.inputConfig = inputConfig;
    }

    public void start() throws IOException {
        logger.info("Start connecting to input source", inputConfig);
        inputConfig.connect();
    }
}
