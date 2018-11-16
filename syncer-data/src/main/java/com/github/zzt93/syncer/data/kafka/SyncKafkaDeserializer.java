package com.github.zzt93.syncer.data.kafka;

import com.github.zzt93.syncer.data.SyncData;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * @author zzt
 */
public class SyncKafkaDeserializer implements Deserializer<SyncData> {
  private static final Gson gson = new Gson();

  @Override
  public void configure(Map<String, ?> map, boolean b) {
  }

  @Override
  public SyncData deserialize(String s, byte[] bytes) {
    return gson.fromJson(new String(bytes), SyncData.class);
  }

  @Override
  public void close() {
  }
}
