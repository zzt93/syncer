package com.github.zzt93.syncer.config.consumer.output.habse;

import com.github.zzt93.syncer.config.ConsumerConfig;
import com.github.zzt93.syncer.config.common.HBaseConnection;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.consumer.output.BufferedOutputChannelConfig;
import com.github.zzt93.syncer.config.syncer.SyncerOutputMeta;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.hbase.HBaseChannel;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.util.StringUtils;

/**
 * @author zzt
 */
@Getter
@Setter
@ToString
@ConsumerConfig("output.hBase")
public class HBase extends BufferedOutputChannelConfig {

  @ConsumerConfig
  private HBaseConnection connection;

  private String consumerId;

  @Override
  public String getConsumerId() {
    return consumerId;
  }

  @Override
  public HBaseChannel toChannel(String consumerId, Ack ack,
                                SyncerOutputMeta outputMeta) throws Exception {

    this.consumerId = consumerId;
    if (!StringUtils.isEmpty(connection) && connection.valid()) {
      return new HBaseChannel(this, outputMeta, ack);
    }
    throw new InvalidConfigException("Invalid connection configuration: " + connection);
  }
}
