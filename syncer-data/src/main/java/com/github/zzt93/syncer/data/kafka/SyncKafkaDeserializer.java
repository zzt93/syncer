package com.github.zzt93.syncer.data.kafka;

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
    return gson.fromJson(new String(bytes), SyncResult.class);
  }

  @Override
  public void close() {
  }
}
