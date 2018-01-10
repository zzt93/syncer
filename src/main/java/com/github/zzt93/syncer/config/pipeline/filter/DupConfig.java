package com.github.zzt93.syncer.config.pipeline.filter;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zzt
 */
public class DupConfig {

  private List<String> copyValue = Lists.newArrayList();
  private List<List<String>> New = new ArrayList<>();

  public List<List<String>> getNew() {
    return New;
  }

  public void setNew(List<List<String>> aNew) {
    New = aNew;
  }

  public List<String> getCopyValue() {

    return copyValue;
  }

  public void setCopyValue(List<String> copyValue) {
    this.copyValue = copyValue;
  }
}
