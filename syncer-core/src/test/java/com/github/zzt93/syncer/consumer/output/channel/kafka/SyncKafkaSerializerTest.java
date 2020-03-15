package com.github.zzt93.syncer.consumer.output.channel.kafka;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncDataTestUtil;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class SyncKafkaSerializerTest {

  private SyncKafkaSerializer serializer;

  @Before
  public void setUp() throws Exception {
    this.serializer = new SyncKafkaSerializer();
  }
  @Test
  public void serialize() {
    SyncData write = SyncDataTestUtil.write("serial", "serial");
    String key = "key";
    // a value that can't represent by double
    long value = ((long) Math.pow(2, 53)) + 1;
    assertNotEquals(value, (double)value);

    write.addField(key, value);
    byte[] serialize = serializer.serialize("", write.getResult());
    // event type in int
    // null fields ignore
    // long as string
    assertEquals("{\"fields\":{\"key\":\"9007199254740993\"},\"eventType\":0,\"repo\":\"serial\",\"entity\":\"serial\",\"id\":\"1234\",\"primaryKeyName\":\"id\"}"
        , new String(serialize));
  }
}