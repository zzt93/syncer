package com.github.zzt93.syncer.data.kafka;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncDataTestUtil;
import com.github.zzt93.syncer.consumer.output.channel.kafka.DeprecatedSyncKafkaSerializer;
import com.github.zzt93.syncer.consumer.output.channel.kafka.SyncKafkaSerializer;
import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.util.NumTestUtil;
import lombok.Getter;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Set;

import static org.junit.Assert.*;

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
    // a value that can't represent by double
    long value = NumTestUtil.notDoubleLong();

    write.addField(Temp.KEY, value).addField(Temp.NAME, Temp.NAME);

    byte[] serialize = serializer.serialize("", write.getResult());
    JsonSyncResult deserialize = jsonKafkaDeserializer.deserialize("", serialize);
    Temp temp = deserialize.getFields(Temp.class);
    assertEquals(value, temp.getKey());
    assertEquals(Temp.NAME, temp.getName());
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
    long value = NumTestUtil.notDoubleLong();

    update.addField(key, value).addField(name, Temp.NAME).addField("i", 1);
    update.addExtra(key, value).addExtra(name, Temp.NAME);
    byte[] serialize = serializer.serialize("", update.getResult());

    JsonSyncResult deserialize = jsonKafkaDeserializer.deserialize("", serialize);
    Temp field = deserialize.getFields(Temp.class);
    Temp before = deserialize.getBefore(Temp.class);
    assertEquals(value, field.getKey());
    assertEquals(name, field.getName());
    assertEquals(SyncDataTestUtil.ID, field.getId());
    assertEquals(SyncDataTestUtil.ID, before.getId());
    assertEquals(SyncDataTestUtil.ID, deserialize.getIdAsLong().longValue());
    assertEquals(SimpleEventType.UPDATE, deserialize.getEventType());
  }

  @Test
  public void testBackCompatible() {
    long id = 12345678;
    SyncData update = SyncDataTestUtil.update("serial", "serial").setId(id);
    // a value that can't represent by double
    long value = NumTestUtil.notDoubleLong();

    long time=System.currentTimeMillis();
    Timestamp timestamp=new Timestamp(time);
    String timeStr=timestamp.toString();
    update.addField(Temp.KEY, value).addField(Temp.NAME, Temp.NAME).addField(Temp.I, 1).addField(Temp.FIRST_NAME, Temp.NAME).addField(Temp.IDE, Temp.NAME).addField(Temp.CREATE_TIME, time).addField(Temp.MODIFY_TIME, timeStr);
    new SyncDataTestUtil().addBefore(update, Temp.NAME, Temp.NAME).addBefore(update, Temp.I, 1).addUpdated(update);

    update.addField(Temp.UPDATED, update.getUpdated()).addField(Temp.BEFORE, update.getBefore());
    update.addExtra(Temp.KEY, value).addExtra(Temp.NAME, Temp.NAME);


    byte[] serialize = deprecatedSyncKafkaSerializer.serialize("", update.getResult());
    JsonSyncResult deserialize = jsonKafkaDeserializer.deserialize("", serialize);
    Temp field = deserialize.getFields(Temp.class);
    Temp before = deserialize.getBefore(Temp.class);
    assertEquals(value, field.getKey());
    assertEquals(Temp.NAME, field.getName());
    assertEquals(timestamp, field.getCreateTime());
    assertEquals(timestamp, field.getModifyTime());
    assertEquals(Temp.NAME, field.getFirstName());
    assertEquals(id, field.getId());
    assertEquals(id, before.getId());
    assertEquals(id, deserialize.getIdAsLong().longValue());
    assertEquals(5, field.getUpdated().size());
		assertFalse(field.getUpdated().contains(Temp.NAME));
		assertFalse(field.getUpdated().contains(Temp.I));
    assertNotNull(field.getBefore());
    assertEquals(Temp.NAME, field.getBefore().getName());
    assertEquals(1, field.getBefore().getI());

    assertEquals(SimpleEventType.UPDATE, deserialize.getEventType());
  }


	@Test
  public void testConvert() {
    long id = 12345678;
    SyncData update = SyncDataTestUtil.update("serial", "serial").setId(id);

    long time=System.currentTimeMillis();
    Timestamp timestamp=new Timestamp(time);
    update.addField(Temp.NAME, Temp.NAME).addField(Temp.I, 1).addField(Temp.FIRST_NAME, Temp.NAME).addField(Temp.IDE, Temp.NAME).addField(Temp.CREATE_TIME,time).addField(Temp.MODIFY_TIME,timestamp);
    new SyncDataTestUtil().addBefore(update, Temp.NAME, Temp.NAME).addBefore(update, Temp.I, 1).addUpdated(update);

		update.addField(Temp.UPDATED, update.getUpdated()).addField(Temp.BEFORE, update.getBefore());
    update.addExtra(Temp.NAME, Temp.NAME).addExtra(Temp.FIRST_NAME, Temp.NAME);


    byte[] serialize = serializer.serialize("", update.getResult());
    JsonSyncResult deserialize = jsonKafkaDeserializer.deserialize("", serialize);
    Temp field = deserialize.getFields(Temp.class);
    assertEquals(timestamp,field.getCreateTime());
    assertEquals(timestamp,field.getModifyTime());
    assertEquals(id, field.getId());
    assertEquals(Temp.NAME, field.getFirstName());
    assertEquals(Temp.NAME, field.getIde());
    assertEquals(4, field.getUpdated().size());
    assertFalse(field.getUpdated().contains(Temp.NAME));
    assertFalse(field.getUpdated().contains(Temp.I));
    assertNotNull(field.getBefore());
    assertEquals(Temp.NAME, field.getBefore().getName());
    assertEquals(1, field.getBefore().getI());

  }

  @Getter
  private static class Temp {
    public static final String FIRST_NAME = "first_name";
    public static final String IDE = "_ide";
    public static final String CREATE_TIME = "create_time";
    public static final String MODIFY_TIME = "modify_time";
    public static final String UPDATED = "updated";
    public static final String BEFORE = "before";
		public static final String KEY = "key";
		public static final String I = "i";
		static final String NAME = "name";
    private long key;
    private String name;
    private long id;
    private int i;
    private String firstName;
    private String Ide;
    private Timestamp createTime;
    private Timestamp modifyTime;
    private Set<String> updated;
    private Temp before;
  }

}