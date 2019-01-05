package com.github.zzt93.syncer.consumer.output.channel.jdbc;

import com.github.zzt93.syncer.common.expr.ParameterReplace;
import java.util.HashMap;
import org.junit.Assert;
import org.junit.Test;

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

}