package com.github.zzt93.syncer.consumer.output.channel.kafka;

import com.github.zzt93.syncer.common.data.SyncResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.LongSerializationPolicy;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @author zzt
 */
public class SyncKafkaSerializer implements Serializer<SyncResult> {
  private static Gson gson = new GsonBuilder().setLongSerializationPolicy(LongSerializationPolicy.STRING).create();

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public byte[] serialize(String topic, SyncResult data) {
    return gson.toJson(data).getBytes();
  }

  @Override
  public void close() {
  }

}
