package com.github.zzt93.syncer.config.consumer.filter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zzt
 */
public class Switcher {

  public static final String DEFAULT = "default";
  private String Switch;
  private Map<String, List<Map>> Case = new HashMap<>();

  public String getSwitch() {
    return Switch;
  }

  public void setSwitch(String aSwitch) {
    this.Switch = aSwitch;
  }

  public Map<String, List<Map>> getCase() {
    return Case;
  }

  public void setCase(Map<String, List<Map>> aCase) {
    this.Case = aCase;
  }
}
