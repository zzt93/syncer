package com.github.zzt93.syncer.consumer.output.channel.kafka;

import com.github.zzt93.syncer.common.data.SyncResult;
import com.github.zzt93.syncer.data.SimpleEventType;
import com.google.gson.*;
import org.apache.kafka.common.serialization.Serializer;

import java.sql.Timestamp;
import java.util.Map;

/**
 * @author zzt
 */
public class SyncKafkaSerializer implements Serializer<SyncResult> {
  private static final Gson gson = new GsonBuilder().setLongSerializationPolicy(LongSerializationPolicy.STRING)
      .registerTypeAdapter(Timestamp.class, (JsonSerializer<Timestamp>) (timestamp, type, jsonSerializationContext) -> new JsonPrimitive(timestamp.getTime()))
      .registerTypeHierarchyAdapter(SimpleEventType.class, SimpleEventType.defaultSerializer)
      .create();

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
