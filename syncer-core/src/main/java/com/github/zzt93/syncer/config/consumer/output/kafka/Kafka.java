package com.github.zzt93.syncer.config.consumer.output.kafka;

import com.github.zzt93.syncer.config.consumer.common.ClusterConnection;
import com.github.zzt93.syncer.config.consumer.common.InvalidConfigException;
import com.github.zzt93.syncer.config.consumer.output.BufferedOutputChannelConfig;
import com.github.zzt93.syncer.config.consumer.output.PipelineBatchConfig;
import com.github.zzt93.syncer.config.syncer.SyncerOutputMeta;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.kafka.KafkaChannel;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zzt
 */
public class Kafka extends BufferedOutputChannelConfig {

  private ClusterConnection connection;
  private MsgMapping msgMapping;
  private String consumerId;

  public ClusterConnection getConnection() {
    return connection;
  }

  public void setConnection(ClusterConnection connection) {
    this.connection = connection;
  }

  public MsgMapping getMsgMapping() {
    return msgMapping;
  }

  public void setMsgMapping(MsgMapping msgMapping) {
    this.msgMapping = msgMapping;
  }

  @Override
  public KafkaChannel toChannel(String consumerId, Ack ack, SyncerOutputMeta outputMeta) throws Exception {
    this.consumerId = consumerId;
    if (connection.valid()) {
      return new KafkaChannel(this, outputMeta, ack);
    }
    throw new InvalidConfigException("Invalid connection configuration: " + connection);
  }

  @Override
  public String getConsumerId() {
    return consumerId;
  }

  public Map<String, Object> buildProperties() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connection.getClusterNodes());
    PipelineBatchConfig batch = getBatch();
    properties.put(ProducerConfig.RETRIES_CONFIG, batch.getMaxRetry());
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, batch.getSize());
    /*
     * Number of acknowledgments the producer requires the leader to have received
     * before considering a request complete.
     */
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    if (batch.getBufferMemory() != null) {
      properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, batch.getBufferMemory());
    }

    /*
     * ID to pass to the server when making requests. Used for server-side logging.
     */
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, consumerId);

    msgMapping.buildProperties(properties);
    return properties;
  }


}
