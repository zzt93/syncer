package com.github.zzt93.syncer.data.kafka;

import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.data.SyncResult;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * @author zzt
 */
public class SyncKafkaDeserializer implements Deserializer<SyncResult> {
  private static final Gson gson = new Gson();

  @Override
  public void configure(Map<String, ?> map, boolean b) {
  }

  @Override
  public SyncResult deserialize(String s, byte[] bytes) {
    try {
      return gson.fromJson(new String(bytes), SyncResult.class);
    } catch (Throwable e) {
      Map map = gson.fromJson(new String(bytes), Map.class);
      SyncResult res = new SyncResult();
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
