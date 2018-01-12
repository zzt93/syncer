package com.github.zzt93.syncer.output.channel.jdbc;

import com.github.zzt93.syncer.common.expr.ParameterReplace;
import java.util.HashMap;
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
    System.out.println(ParameterReplace.orderedParam("(?0)", s.substring(1, s.length() - 1)));
    String s1 = map.keySet().toString();
    String s2 = map.values().toString();
    System.out.println(ParameterReplace.orderedParam("(?0), (?1)",
        s1.substring(1, s1.length() - 1), s2.substring(1, s2.length() - 1)));
  }

}