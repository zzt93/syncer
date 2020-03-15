package com.github.zzt93.syncer.data.kafka;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncDataTestUtil;
import com.github.zzt93.syncer.consumer.output.channel.kafka.SyncKafkaSerializer;
import com.github.zzt93.syncer.data.SimpleEventType;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RawMapSyncResultTest {

  private SyncKafkaSerializer serializer;
  private MapKafkaDeserializer mapKafkaDeserializer;

  @Before
  public void setUp() throws Exception {
    this.serializer = new SyncKafkaSerializer();
    mapKafkaDeserializer = new MapKafkaDeserializer();
  }

  @Test
  public void getFieldAsLong() {
    SyncData write = SyncDataTestUtil.write("serial", "serial");
    String key = "key";
    // a value that can't represent by double
    long value = ((long) Math.pow(2, 53)) + 1;

    write.addField(key, value);
    byte[] serialize = serializer.serialize("", write.getResult());
    RawMapSyncResult deserialize = mapKafkaDeserializer.deserialize("", serialize);
    long jsonValue = deserialize.getFieldAsLong(key);
    assertEquals(value, jsonValue);
    assertEquals(deserialize.getEventType(), SimpleEventType.WRITE);
    assertEquals(SyncDataTestUtil.ID, deserialize.getIdAsLong().longValue());
    assertEquals(SimpleEventType.WRITE, deserialize.getEventType());
  }
}