package com.github.zzt93.syncer.consumer.output.channel.jdbc;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncDataTestUtil;
import com.github.zzt93.syncer.common.expr.ParameterReplace;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.github.zzt93.syncer.consumer.output.channel.jdbc.SQLMapper.*;

/**
 * @author zzt
 */
@Slf4j
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
    String expected = ParameterReplace.orderedParam(INSERT_INTO_VALUES, "test", "table", "title", "'?4'");
    Assert.assertEquals("insert into `test`.`table` (title) values ('?4')", expected);
  }

  @Test
  public void delete() {
    String expected = ParameterReplace.orderedParam(DELETE_FROM_WHERE_ID, "test", "table", "1");
    Assert.assertEquals("delete from `test`.`table` where id = 1", expected);
  }

  private static final ExecutorService ex = Executors.newFixedThreadPool(1);

  @Test
  public void concurrentMap() throws InterruptedException {
    SyncData s = SyncDataTestUtil.write("test", "test");
    for (int i = 0; i < 100; i++) {
      s.addField("" + i, i);
    }

    CountDownLatch latch = new CountDownLatch(1);
    CountDownLatch end = new CountDownLatch(1);
    AtomicBoolean hasError = new AtomicBoolean(false);
    ex.submit(() -> {
      latch.countDown();
      for (int i = 0; i < 10000; i++) {
        try {
          consume(s.toString());
        } catch (Exception e) {
          hasError.set(true);
        }
      }
      end.countDown();
    });
    SQLMapper sqlMapper = new SQLMapper();
    latch.await();
    for (int i = 0; i < 10000; i++) {
      String map = sqlMapper.map(s);
      consume(map);
    }
    end.await();

    Assert.assertFalse(hasError.get());
  }

  private void consume(String s) {
    if (s.length() != 0) {
      Math.sqrt(s.length());
    }
  }
}