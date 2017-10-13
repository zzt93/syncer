package com.github.zzt93.syncer.config.pipeline.filter;

import java.util.List;

/**
 * @author zzt
 */
public class CloneConfig {

  private List<String> fields;
  private List<String> New;
  private List<String> old;

  public List<String> getFields() {
    return fields;
  }

  public void setFields(List<String> fields) {
    this.fields = fields;
  }

  public List<String> getNew() {
    return New;
  }

  public void setNew(List<String> aNew) {
    New = aNew;
  }

  public List<String> getOld() {
    return old;
  }

  public void setOld(List<String> old) {
    this.old = old;
  }
}
