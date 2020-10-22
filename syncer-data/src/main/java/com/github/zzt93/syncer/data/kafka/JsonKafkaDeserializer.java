package com.github.zzt93.syncer.data.kafka;

import com.github.zzt93.syncer.data.SimpleEventType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonObject;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * @author zzt
 */
public class JsonKafkaDeserializer implements Deserializer<JsonSyncResult> {
  private static final Gson gson = new GsonBuilder()
      .registerTypeAdapter(JsonObject.class, (JsonDeserializer<JsonObject>) (jsonElement, type, jsonDeserializationContext) -> jsonElement.isJsonObject() ? jsonElement.getAsJsonObject() : new JsonObject())
      .registerTypeHierarchyAdapter(SimpleEventType.class, SimpleEventType.defaultDeserializer)
      .create();


  @Override
  public void configure(Map<String, ?> map, boolean b) {
  }

  @Override
  public JsonSyncResult deserialize(String s, byte[] bytes) {
    return gson.fromJson(new String(bytes), JsonSyncResult.class);
  }

  @Override
  public void close() {
  }
}
