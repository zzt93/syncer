package com.github.zzt93.syncer.common.data;

import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.producer.dispatch.mysql.event.NamedFullRow;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.junit.Before;
import org.junit.Test;

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
    assertTrue(false);
  }
}