package com.github.zzt93.syncer.common.data;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * @author zzt
 */
@AllArgsConstructor
@Slf4j
public class ExtraQueryField {
  private final ExtraQuery extraQuery;
  private final String fieldName;

  public Object getQueryResult(String key) {
    if (!fieldName.equals(key)) {
      log.error("Not matched {}, {}", fieldName, key);
    }
    return extraQuery.getQueryResult(fieldName);
  }

  public Object getQueryResult() {
    return extraQuery.getQueryResult(fieldName);
  }

  public String toString() {
    return "ExtraQueryField{" + fieldName + "=" + extraQuery.toString() + "}";
  }
}
