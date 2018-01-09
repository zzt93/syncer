package com.github.zzt93.syncer.consumer.output.channel.jdbc;

import com.github.zzt93.syncer.common.expr.ParameterReplace;
import java.util.HashMap;
import java.util.stream.Collectors;

/**
 * @author zzt
 */
public class ParameterizedString {

  private String sql;
  private HashMap<String, String> nameToAlias = new HashMap<>();

  public ParameterizedString(String sql) {
    this.sql = sql;
  }

  public String getSql() {
    String collect = nameToAlias.entrySet().stream().map(e -> e.getKey() + " as " + e.getValue())
        .collect(Collectors.joining(","));
    return ParameterReplace.orderedParam(sql, collect);
  }

  public void nameToAlias(String key, String value) {
    nameToAlias.put(key, value);
  }

  public void nameToAlias(HashMap<String, String> tmp) {
    nameToAlias.putAll(tmp);
  }
}
