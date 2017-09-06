package com.github.zzt93.syncer.config.output;

import com.github.zzt93.syncer.output.OutputChannel;

/**
 * @author zzt
 */
public interface OutputChannelConfig {

    void connect();

    OutputChannel build();
}
