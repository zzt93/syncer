package com.github.zzt93.syncer.filter.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
public class Action {

  private Map<Object, List<String>> action = new HashMap<>();

  public Map<Object, List<String>> getAction() {
    return action;
  }

  public void setAction(Map<Object, List<String>> action) {
    this.action = action;
  }
}
