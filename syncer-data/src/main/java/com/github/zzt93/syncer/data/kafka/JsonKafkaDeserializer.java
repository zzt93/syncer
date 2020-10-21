package com.github.zzt93.syncer.data.kafka;

import com.github.zzt93.syncer.data.SimpleEventType;
import com.google.gson.*;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * @author zzt
 */
public class JsonKafkaDeserializer implements Deserializer<JsonSyncResult> {
  private static final Gson gson = new GsonBuilder()
      .registerTypeAdapter(String.class, new MapDeserializer())
      .registerTypeAdapter(JsonObject.class, (JsonDeserializer<JsonObject>) (jsonElement, type, jsonDeserializationContext) -> jsonElement.isJsonObject() ? jsonElement.getAsJsonObject() : new JsonObject())
      .registerTypeHierarchyAdapter(SimpleEventType.class, SimpleEventType.defaultDeserializer)
      .create();

  private static class MapDeserializer implements JsonDeserializer<String> {
    public String deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
        throws JsonParseException {
      if (json.isJsonObject()) {
        return json.toString();
      }
      return json.getAsString();
    }
  }

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
