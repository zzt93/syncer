package com.github.zzt93.syncer.config.pipeline.output;

import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;

/**
 * @author zzt
 */
public interface OutputChannelConfig {

  OutputChannel toChannel() throws Exception;

}
