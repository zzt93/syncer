package com.github.zzt93.syncer.data.kafka;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncDataTestUtil;
import com.github.zzt93.syncer.consumer.output.channel.kafka.DeprecatedSyncKafkaSerializer;
import com.github.zzt93.syncer.consumer.output.channel.kafka.SyncKafkaSerializer;
import com.github.zzt93.syncer.data.SimpleEventType;
import lombok.Getter;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class JsonSyncResultTest {

  private SyncKafkaSerializer serializer;
  private DeprecatedSyncKafkaSerializer deprecatedSyncKafkaSerializer;
  private JsonKafkaDeserializer jsonKafkaDeserializer;

  @Before
  public void setUp() {
    this.serializer = new SyncKafkaSerializer();
    deprecatedSyncKafkaSerializer = new DeprecatedSyncKafkaSerializer();
    jsonKafkaDeserializer = new JsonKafkaDeserializer();
  }

  @Test
  public void getFields() {
    SyncData write = SyncDataTestUtil.write("serial", "serial");
    String key = "key";
    String name = "name";
    // a value that can't represent by double
    long value = ((long) Math.pow(2, 53)) + 1;
    assertNotEquals(value, (double) value);

    write.addField(key, value).addField(name, Temp.NAME);
    byte[] serialize = serializer.serialize("", write.getResult());
    JsonSyncResult deserialize = jsonKafkaDeserializer.deserialize("", serialize);
    Temp temp = deserialize.getFields(Temp.class);
    assertEquals(value, temp.getKey());
    assertEquals(name, temp.getName());
    assertEquals(SyncDataTestUtil.ID, temp.getId());
    assertEquals(SyncDataTestUtil.ID, deserialize.getIdAsLong().longValue());
    assertEquals(SimpleEventType.WRITE, deserialize.getEventType());
  }

  @Test
  public void testGetFieldsUpdate() {
    SyncData update = SyncDataTestUtil.update("serial", "serial");
    String key = "key";
    String name = "name";
    // a value that can't represent by double
    long value = ((long) Math.pow(2, 53)) + 1;
    assertNotEquals(value, (double) value);

    update.addField(key, value).addField(name, Temp.NAME).addField("i", 1);
    update.addExtra(key, value).addExtra(name, Temp.NAME);
    byte[] serialize = serializer.serialize("", update.getResult());
    JsonSyncResult deserialize = jsonKafkaDeserializer.deserialize("", serialize);
    Temp field = deserialize.getFields(Temp.class);
    Temp before = deserialize.getBefore(Temp.class);
    Temp extras = deserialize.getExtras(Temp.class);
    assertEquals(value, field.getKey());
    assertEquals(name, field.getName());
    assertEquals(value, extras.getKey());
    assertEquals(name, extras.getName());
    assertEquals(SyncDataTestUtil.ID, field.getId());
    assertEquals(SyncDataTestUtil.ID, before.getId());
    assertEquals(SyncDataTestUtil.ID, deserialize.getIdAsLong().longValue());
    assertEquals(SimpleEventType.UPDATE, deserialize.getEventType());
  }

  @Test
  public void testBackCompatible() {
    long id = 12345678;
    SyncData update = SyncDataTestUtil.update("serial", "serial").setId(id);
    String key = "key";
    String name = "name";
    // a value that can't represent by double
    long value = ((long) Math.pow(2, 53)) + 1;
    assertNotEquals(value, (double) value);

    update.addField(key, value).addField(name, Temp.NAME).addField("i", 1);
    update.addExtra(key, value).addExtra(name, Temp.NAME);
    byte[] serialize = deprecatedSyncKafkaSerializer.serialize("", update.getResult());
    JsonSyncResult deserialize = jsonKafkaDeserializer.deserialize("", serialize);
    Temp field = deserialize.getFields(Temp.class);
    Temp before = deserialize.getBefore(Temp.class);
    Temp extras = deserialize.getExtras(Temp.class);
    assertEquals(value, field.getKey());
    assertEquals(name, field.getName());
    assertEquals(id, field.getId());
    assertEquals(value, extras.getKey());
    assertEquals(name, extras.getName());
    assertEquals(id, before.getId());
    assertEquals(id, deserialize.getIdAsLong().longValue());
    assertEquals(SimpleEventType.UPDATE, deserialize.getEventType());
  }

  @Test
  public void testConvert() {
    long id = 12345678;
    SyncData update = SyncDataTestUtil.update("serial", "serial").setId(id);
    String name = "name";

    update.addField(name, Temp.NAME).addField("i", 1).addField(Temp.FIRST_NAME, name).addField(Temp.IDE, name);
    update.addExtra(name, Temp.NAME).addExtra(Temp.FIRST_NAME, name);
    byte[] serialize = serializer.serialize("", update.getResult());
    JsonSyncResult deserialize = jsonKafkaDeserializer.deserialize("", serialize);
    Temp field = deserialize.getFields(Temp.class);
    Temp extras = deserialize.getExtras(Temp.class);
    assertEquals(id, field.getId());
    assertEquals(name, field.getFirstName());
    assertEquals(name, field.getIde());
    assertEquals(name, extras.getFirstName());
  }

  @Getter
  private static class Temp {
    public static final String FIRST_NAME = "first_name";
    public static final String IDE = "_ide";
    static final String NAME = "name";
    private long key;
    private String name;
    private long id;
    private int i;
    private String firstName;
    private String Ide;
  }

}