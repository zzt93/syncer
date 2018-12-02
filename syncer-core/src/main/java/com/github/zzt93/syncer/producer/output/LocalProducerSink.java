package com.github.zzt93.syncer.producer.output;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.consumer.ConsumerSource;

/**
 * @author zzt
 */
public class LocalProducerSink implements ProducerSink {

  private ConsumerSource consumerSource;

  public LocalProducerSink(ConsumerSource consumerSource) {
    this.consumerSource = consumerSource;
  }

  @Override
  public boolean output(SyncData data) {
    return consumerSource.input(data);
  }

  @Override
  public boolean output(SyncData[] data) {
    return consumerSource.input(data);
  }

  @Override
  public ConsumerSource remote() {
    return consumerSource;
  }


  @Override
  public String toString() {
    return "LocalProducerSink{" +
        "consumerSource=" + consumerSource +
        '}';
  }
}
