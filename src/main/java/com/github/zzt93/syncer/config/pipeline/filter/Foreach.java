package com.github.zzt93.syncer.config.pipeline.filter;

import java.util.List;

/**
 * @author zzt
 */
public class Foreach {

  private String var;
  private String in;
  private List<String> statement;

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

  public List<String> getStatement() {
    return statement;
  }

  public void setStatement(List<String> statement) {
    this.statement = statement;
  }
}
