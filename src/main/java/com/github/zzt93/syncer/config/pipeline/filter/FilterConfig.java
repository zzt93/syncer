package com.github.zzt93.syncer.config.pipeline.filter;

import java.util.List;

/**
 * @author zzt
 */
public class FilterConfig {

  private FilterType type;
  private Switcher switcher;
  private List<String> statement;
  private ForeachConfig foreach;
  private IfConfig If;

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

  public ForeachConfig getForeach() {
    return foreach;
  }

  public void setForeach(ForeachConfig foreach) {
    this.foreach = foreach;
    type = FilterType.FOREACH;
  }

  public IfConfig getIf() {
    return If;
  }

  public void setIf(IfConfig anIf) {
    this.If = anIf;
    type = FilterType.IF;
  }

  private void typeCheck() {
    if (type != null) {
      throw new IllegalArgumentException("Invalid config to combine several filter type");
    }
  }

  public enum FilterType {
    SWITCH, STATEMENT, FOREACH, IF
  }
}
