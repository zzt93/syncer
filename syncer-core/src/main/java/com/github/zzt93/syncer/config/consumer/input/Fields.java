package com.github.zzt93.syncer.config.consumer.input;

import com.github.zzt93.syncer.config.common.InvalidConfigException;

import java.util.HashSet;
import java.util.List;

public class Fields {

  private static final String _all = "-all-";
  private final HashSet<String> names;

  Fields(List<String> names) {
    this.names = new HashSet<>(names);
    if (this.names.size() != names.size()) {
      throw new InvalidConfigException("Dup column config: " +  names);
    }
  }

  public boolean contains(String o) {
    return names.contains(o);
  }

}
