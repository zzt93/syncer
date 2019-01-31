package com.github.zzt93.syncer.config.consumer.filter;

import com.google.gson.annotations.SerializedName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zzt
 */
public class Switcher {

  public static final String DEFAULT = "default";
  @SerializedName(value = "switch", alternate = {"Switch"})
  private String Switch;
  @SerializedName(value = "case", alternate = {"Case"})
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
