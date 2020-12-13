package com.github.zzt93.syncer.consumer.output.channel.jdbc;

import com.github.zzt93.syncer.common.expr.ParameterReplace;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

import static com.github.zzt93.syncer.consumer.output.channel.jdbc.SQLMapper.*;

/**
 * @author zzt
 */
public class SQLMapperTest {

  @Test
  public void replacePlaceholders() throws Exception {
    HashMap<String, Object> map = new HashMap<>();
    map.put("1", 1);
    map.put("2", 1);
    map.put("3", 3);
    map.put("4", 4);
    map.put("5", -1);
    String s = map.toString();
    Assert.assertEquals(ParameterReplace.orderedParam("(?0)", s.substring(1, s.length() - 1)),
        "(1=1, 2=1, 3=3, 4=4, 5=-1)");
    String key = map.keySet().toString();
    String value = map.values().toString();
    Assert.assertEquals(ParameterReplace.orderedParam("(?0), (?1)",
        key.substring(1, key.length() - 1), value.substring(1, value.length() - 1)), "(1, 2, 3, 4, 5), (1, 1, 3, 4, -1)");
  }

  @Test
  public void noNeedEscape() {
    HashMap<String, Object> map = new HashMap<>();
    map.put("1", 1);
    map.put("?0", 3);
    map.put("2", 1);
    map.put("4", 4);
    map.put("5", -1);
    String key = map.keySet().toString();
    String value = map.values().toString();
    String res = ParameterReplace.orderedParam("(?0), (?1)",
        key.substring(1, key.length() - 1), value.substring(1, value.length() - 1));
    Assert.assertEquals("(1, ?0, 2, 4, 5), (1, 3, 1, 4, -1)", res);
    res = ParameterReplace.orderedParam("(?1), (?0)",
        key.substring(1, key.length() - 1), value.substring(1, value.length() - 1));
    Assert.assertEquals("(1, 3, 1, 4, -1), (1, ?0, 2, 4, 5)", res);
  }

  @Test
  public void updateById() {
    String expected = ParameterReplace.orderedParam(UPDATE_SET_WHERE_ID, "test", "table", "1", "title='?2'");
    Assert.assertEquals("update `test`.`table` set title='?2' where id = 1", expected);
  }

  @Test
  public void updateByWhere() {
    String expected = ParameterReplace.orderedParam(UPDATE_SET_WHERE, "test", "table", "id = 1", "title='?2'");
    Assert.assertEquals("update `test`.`table` set title='?2' where id = 1", expected);
  }

  @Test
  public void insert() {
    String expected = ParameterReplace.orderedParam(INSERT_INTO_VALUES, "test", "table", "title", "'?4'", "id", "id");
    Assert.assertEquals("insert into `test`.`table` (title) values ('?4') ON DUPLICATE KEY UPDATE id=id", expected);
  }

  @Test
  public void delete() {
    String expected = ParameterReplace.orderedParam(DELETE_FROM_WHERE_ID, "test", "table", "1");
    Assert.assertEquals("delete from `test`.`table` where id = 1", expected);
  }

}