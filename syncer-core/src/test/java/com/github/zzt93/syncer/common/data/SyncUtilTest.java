package com.github.zzt93.syncer.common.data;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.zzt93.syncer.data.util.SyncUtil;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zzt
 */
public class SyncUtilTest {

  @Test
  public void fromJson() throws Exception {
    Object o = SyncUtil.fromJson("[\"AC\",\"BD\",\"CE\",\"DF\",\"GG\"]", String[].class);
    Assert.assertEquals(o.getClass(), String[].class);
    String[] ss = (String[]) o;
    Assert.assertEquals(ss.length, 5);
    Object map = SyncUtil.fromJson("{a:1, b:\"asd\"}", Map.class);
    Map m = (Map) map;
    Assert.assertEquals(m.size(), 2);
    Assert.assertEquals(m.get("a"), 1.0);
    Assert.assertEquals(m.get("b"), "asd");
  }

  @Test
  public void announcementBlock() throws Exception {
    Map o = (Map) SyncUtil.fromJson(
        "{\"blocks\":[{\"data\":{},\"depth\":0,\"entityRanges\":[],\"inlineStyleRanges\":[],\"key\":\"ummxd\",\"text\":\"Test\",\"type\":\"unstyled\"}],\"entityMap\":{}}",
        Map.class);
    List<Map> blocks = ((List<Map>) o.get("blocks"));
    Assert.assertEquals(blocks.size(), 1);
    Map map = blocks.get(0);
    String text = (String) map.get("text");
    Assert.assertEquals(text, "Test");
  }

  @Test
  public void testWithToken() {
    TypeToken<Map<String, String>> token = new TypeToken<Map<String, String>>() {
    };
    Map<String, String> map = SyncUtil.fromJson("{ \"_id\" : \"EuFB$vwXpMPb\", \"_key\" : \"L76y$\", \"tp\" : 3, \"sub\" : 0, \"name\" : \"测试-熊大飞\", \"time\" : 1547535531778, \"fromUserId\" : NumberLong(260404), \"index\" : 95, \"content\" : \"{\\\"code\\\":2500,\\\"data\\\":\\\"Duplicate key cn.superid.live.form.StartLiveForm$StreamLayout@44f3346f\\\"}\", \"groupId\" : NumberLong(12143021), \"fromRoleId\" : NumberLong(13000005), \"apns\" : \"[]\", \"state\" : 0, \"options\" : \"{\\\"announcementId\\\":13150624}\" }", token);
    Assert.assertTrue(map.containsKey("content"));
    Map<String, String> map1 = SyncUtil.fromJson("{\"a\":{}}", token);
    Assert.assertNull(map1);
    Map<String, String> map2 = SyncUtil.fromJson("{\"a\":[]}", token);
    Assert.assertNull(map2);
  }

  @Test
  public void testRename() {
    Map<String, Object> row = new HashMap<>();
    row.put("a_pub", "1");
    row.put("bPub", "1");
    row.put("c_p_ub", "1");
    row.put("d_p_ub_", "1");
    row.put("e_p_u_b", "1");
    row.put("f_p_u_bB", "1");
    row.put("g_p_u_BB", "1");
    row.put("hPuB", "1");
    SyncData data = new SyncData("asdf", 1, "test", "test", "id", 1L, row, EventType.UPDATE_ROWS);
    SyncUtil.underscoreToCamel(data);
    Assert.assertEquals("1", data.getField("aPub"));
    Assert.assertEquals("1", data.getField("bPub"));
    Assert.assertEquals("1", data.getField("cPUb"));
    Assert.assertEquals("1", data.getField("dPUb"));
    Assert.assertEquals("1", data.getField("ePUB"));
    Assert.assertEquals("1", data.getField("fPUBB"));
    Assert.assertEquals("1", data.getField("gPUBB"));
    Assert.assertEquals("1", data.getField("hPuB"));
  }
}