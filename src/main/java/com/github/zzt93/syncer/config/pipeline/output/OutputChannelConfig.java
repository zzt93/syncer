package com.github.zzt93.syncer.config.pipeline.output;

import com.github.zzt93.syncer.config.syncer.SyncerOutputMeta;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;

/**
 * @author zzt
 */
public interface OutputChannelConfig {

  OutputChannel toChannel(Ack ack,
      SyncerOutputMeta outputMeta) throws Exception;

}
