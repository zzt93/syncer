package com.github.zzt93.syncer.config.pipeline.filter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zzt
 */
public class Switcher {

  public static final String DEFAULT = "default";
  private String Switch;
  private Map<String, List<String>> Case = new HashMap<>();

  public String getSwitch() {
    return Switch;
  }

  public void setSwitch(String aSwitch) {
    this.Switch = aSwitch;
  }

  public Map<String, List<String>> getCase() {
    return Case;
  }

  public void setCase(Map<String, List<String>> aCase) {
    this.Case = aCase;
  }
}
