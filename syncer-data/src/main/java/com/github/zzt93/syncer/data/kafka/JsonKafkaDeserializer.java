package com.github.zzt93.syncer.data.kafka;

import com.github.zzt93.syncer.data.SimpleEventType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * @author zzt
 */
public class JsonKafkaDeserializer implements Deserializer<JsonSyncResult> {
  private static final Gson gson = new GsonBuilder()
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
