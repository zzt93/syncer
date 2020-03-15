package com.github.zzt93.syncer.data.kafka;

import com.github.zzt93.syncer.data.SimpleEventType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * @author zzt
 */
public class MapKafkaDeserializer implements Deserializer<RawMapSyncResult> {
  private static final Gson gson = new GsonBuilder()
      .registerTypeHierarchyAdapter(SimpleEventType.class, SimpleEventType.defaultDeserializer)
      .create();

  @Override
  public void configure(Map<String, ?> map, boolean b) {
  }

  @Override
  public RawMapSyncResult deserialize(String s, byte[] bytes) {
    try {
      return gson.fromJson(new String(bytes), RawMapSyncResult.class);
    } catch (Throwable e) {
      Map map = gson.fromJson(new String(bytes), Map.class);
      RawMapSyncResult res = new RawMapSyncResult();
      res.setEventType(tmp(map));
      res.getFields().putAll((Map<? extends String, ?>) map.get("fields"));
      res.setId(map.get("id"));
      res.setEntity((String) map.get("repo"));
      res.setRepo((String) map.get("entity"));
      return res;
    }
  }

  private SimpleEventType tmp(Map map) {
    String s = ((Map) map.get("inner")).get("type").toString();
    if (s.contains("WRITE")) {
      return SimpleEventType.WRITE;
    }
    if (s.contains("UPDATE")) {
      return SimpleEventType.UPDATE;
    }
    if (s.contains("DELETE")) {
      return SimpleEventType.DELETE;
    }
    throw new IllegalStateException();
  }

  @Override
  public void close() {
  }
}
