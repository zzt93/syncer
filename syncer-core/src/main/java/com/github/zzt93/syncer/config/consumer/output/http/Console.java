package com.github.zzt93.syncer.config.consumer.output.http;


import com.github.zzt93.syncer.config.consumer.output.OutputChannelConfig;
import com.github.zzt93.syncer.config.syncer.SyncerOutputMeta;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.http.ConsoleChannel;
import lombok.Getter;
import lombok.Setter;

/**
 * @author zzt
 */
@Getter
@Setter
public class Console implements OutputChannelConfig {

  private String consumerId;

  @Override
  public String getConsumerId() {
    return consumerId;
  }

  @Override
  public ConsoleChannel toChannel(String consumerId, Ack ack,
                                  SyncerOutputMeta outputMeta) {
    this.consumerId = consumerId;
    return new ConsoleChannel(this, ack);
  }
}
