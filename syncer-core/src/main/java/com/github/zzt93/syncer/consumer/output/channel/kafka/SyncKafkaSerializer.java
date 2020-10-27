package com.github.zzt93.syncer.consumer.output.channel.kafka;

import com.github.zzt93.syncer.common.data.SyncResult;
import com.github.zzt93.syncer.data.SimpleEventType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.LongSerializationPolicy;
import org.apache.kafka.common.serialization.Serializer;

import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.util.Map;

/**
 * @author zzt
 */
public class SyncKafkaSerializer implements Serializer<SyncResult> {
  private static Gson gson = new GsonBuilder().setLongSerializationPolicy(LongSerializationPolicy.STRING)
      .registerTypeAdapter(Timestamp.class, new JsonSerializer<Timestamp>() {
        @Override
        public JsonElement serialize(Timestamp timestamp, Type type, JsonSerializationContext jsonSerializationContext) {
          return new JsonPrimitive(timestamp.getTime());
        }
      })
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
