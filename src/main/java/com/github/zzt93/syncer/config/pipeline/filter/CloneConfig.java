package com.github.zzt93.syncer.config.pipeline.filter;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zzt
 */
public class CloneConfig {

  /**
   * default only clone event type & action, i.e. non-data field
   */
  private List<String> copyValue = Lists.newArrayList();
  private List<String> New = new ArrayList<>();
  private List<String> old = new ArrayList<>();

  public List<String> getCopyValue() {
    return copyValue;
  }

  public void setCopyValue(List<String> copyValue) {
    this.copyValue = copyValue;
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
