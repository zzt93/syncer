package com.github.zzt93.syncer.data.kafka;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncDataTestUtil;
import com.github.zzt93.syncer.consumer.output.channel.kafka.SyncKafkaSerializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SyncResultFromJsonTest {

  private SyncKafkaSerializer serializer;
  private SyncKafkaDeserializer syncKafkaDeserializer;

  @Before
  public void setUp() throws Exception {
    this.serializer = new SyncKafkaSerializer();
    syncKafkaDeserializer = new SyncKafkaDeserializer();
  }

  @Test
  public void getFieldAsLong() {
    SyncData write = SyncDataTestUtil.write("serial", "serial");
    String key = "key";
    // a value that can't represent by double
    long value = ((long) Math.pow(2, 53)) + 1;
    Assert.assertNotEquals(value, (double)value);

    write.addField(key, value);
    byte[] serialize = serializer.serialize("", write.getResult());
    SyncResultFromJson deserialize = syncKafkaDeserializer.deserialize("", serialize);
    long jsonValue = deserialize.getFieldAsLong(key);
    Assert.assertEquals(value, jsonValue);
  }
}