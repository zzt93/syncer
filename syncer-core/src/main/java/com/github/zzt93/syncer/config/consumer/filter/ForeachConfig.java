package com.github.zzt93.syncer.config.consumer.filter;

import java.util.List;

/**
 * @author zzt
 */
public class ForeachConfig {

  private String var;
  private String in;
  private List<FilterConfig> statement;

  public String getVar() {
    return var;
  }

  public void setVar(String var) {
    this.var = var;
  }

  public String getIn() {
    return in;
  }

  public void setIn(String in) {
    this.in = in;
  }

  public List<FilterConfig> getStatement() {
    return statement;
  }

  public void setStatement(List<FilterConfig> statement) {
    this.statement = statement;
  }
}
