package com.github.zzt93.syncer.config.pipeline.filter;

import java.util.List;

/**
 * @author zzt
 */
public class FilterConfig {

  private FilterType type;
  private Switcher switcher;
  private List<String> statement;

  public Switcher getSwitcher() {
    return switcher;
  }

  public void setSwitcher(Switcher switcher) {
    this.switcher = switcher;
    type = FilterType.SWITCH;
  }

  public List<String> getStatement() {
    return statement;
  }

  public void setStatement(List<String> statement) {
    this.statement = statement;
    type = FilterType.STATEMENT;
  }

  public FilterType getType() {
    return type;
  }

  public void setType(FilterType type) {
    this.type = type;
  }

  public enum FilterType {
    SWITCH, STATEMENT
  }
}
