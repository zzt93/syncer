package com.github.zzt93.syncer.data.kafka;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncDataTestUtil;
import com.github.zzt93.syncer.consumer.output.channel.kafka.SyncKafkaSerializer;
import com.github.zzt93.syncer.data.SimpleEventType;
import lombok.Getter;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class JsonSyncResultTest {

  private SyncKafkaSerializer serializer;
  private JsonKafkaDeserializer jsonKafkaDeserializer;

  @Before
  public void setUp() {
    this.serializer = new SyncKafkaSerializer();
    jsonKafkaDeserializer = new JsonKafkaDeserializer();
  }

  @Getter
  private static class Temp {
    static final String NAME = "name";
    private long key;
    private String name;
  }
  @Test
  public void getFields() {
    SyncData write = SyncDataTestUtil.write("serial", "serial");
    String key = "key";
    String name = "name";
    // a value that can't represent by double
    long value = ((long) Math.pow(2, 53)) + 1;
    assertNotEquals(value, (double)value);

    write.addField(key, value).addField(name, Temp.NAME);
    byte[] serialize = serializer.serialize("", write.getResult());
    JsonSyncResult deserialize = jsonKafkaDeserializer.deserialize("", serialize);
    Temp temp = deserialize.getFields(Temp.class);
    assertEquals(value, temp.getKey());
    assertEquals(name, temp.getName());
    assertEquals(SyncDataTestUtil.ID, deserialize.getIdAsLong().longValue());
    assertEquals(SimpleEventType.WRITE, deserialize.getEventType());
  }
}