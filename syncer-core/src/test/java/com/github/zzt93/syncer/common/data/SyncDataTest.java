package com.github.zzt93.syncer.common.data;

import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.producer.dispatch.mysql.event.NamedFullRow;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author zzt
 */
public class SyncDataTest {

  private Gson gson = new Gson();

  @Before
  public void setUp() throws Exception {
  }

  @Test
  public void testSerialize() {
    SyncData data = new SyncData("asdf", 1, SimpleEventType.UPDATE, "test", "test", "id", 1L, new NamedFullRow(Maps.newHashMap()));
    String s = gson.toJson(data);
    SyncData syncData = gson.fromJson(s, SyncData.class);
    assertEquals(data.getEventId(), syncData.getEventId());
    assertEquals(data.getRepo(), syncData.getRepo());
  }

  @Test
  public void testUpdated() {
    HashMap<String, Object> before = new HashMap<>();
    before.put("1", 1);
    before.put("2", "22");
    before.put("3", "33".getBytes());
    before.put("4", 4L);
    before.put("5", new Timestamp(System.currentTimeMillis()));
    before.put("6", "中文".getBytes(StandardCharsets.UTF_8));
    before.put("7", "中文".getBytes(StandardCharsets.UTF_8));
    HashMap<String, Object> now = new HashMap<>();
    now.put("1", 1);
    now.put("2", "2");
    now.put("3", "3".getBytes());
    now.put("4", 4L);
    now.put("5", new Timestamp(System.currentTimeMillis() + 1000));
    now.put("6", "中文".getBytes(StandardCharsets.UTF_8));
    now.put("7", "中文啊".getBytes(StandardCharsets.UTF_8));
    NamedFullRow row = new NamedFullRow(now).setBeforeFull(before);
    SyncData data = new SyncData("asdf", 1, SimpleEventType.UPDATE, "test", "test", "id", 1L, row);
    assertTrue(data.updated());
    assertTrue(!data.updated("1"));
    assertTrue(data.updated("2"));
    assertTrue(data.updated("3"));
    assertTrue(!data.updated("4"));
    assertTrue(data.updated("5"));
    assertTrue(!data.updated("6"));
    assertTrue(data.updated("7"));
  }
}