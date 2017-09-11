package com.github.zzt93.syncer.input;

import com.github.zzt93.syncer.config.share.Connection;
import com.github.zzt93.syncer.config.input.Input;
import com.github.zzt93.syncer.input.connect.MasterConnector;
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
        logger.info("Starting input source", inputConfig);
//        for (Connection connection : inputConfig.getConnections()) {
//            new MasterConnector(connection).connect();
//        }
        logger.info("Stopping syncer");
    }
}
