package com.github.zzt93.syncer.config.consumer.input;

import com.github.zzt93.syncer.config.common.InvalidConfigException;

import java.util.HashSet;
import java.util.List;

public class Fields {

  private static final String _all = "-all-";
  private HashSet<String> names;

  Fields(List<String> names) {
    if (names == null) {
      return;
    }
    this.names = new HashSet<>(names);
    if (this.names.size() != names.size()) {
      throw new InvalidConfigException("Dup column config: " +  names);
    }
  }

  public boolean contains(String o) {
    return names == null || names.contains(o);
  }

  @Override
  public String toString() {
    return "Fields{" +
        "names=" + (names == null ? _all : names) +
        '}';
  }
}
