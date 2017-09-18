package com.github.zzt93.syncer.config.pipeline.filter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zzt
 */
public class FilterConfig {

  private FilterType type;
  private String condition;
  private Map<String, List<String>> action = new HashMap<>();

  public FilterType getType() {
    return type;
  }

  public void setType(FilterType type) {
    this.type = type;
  }

  public String getCondition() {
    return condition;
  }

  public void setCondition(String condition) {
    this.condition = condition;
  }

  public Map<String, List<String>> getAction() {
    return action;
  }

  public void setAction(Map<String, List<String>> action) {
    this.action = action;
  }

  enum FilterType {
    IF
  }
}
