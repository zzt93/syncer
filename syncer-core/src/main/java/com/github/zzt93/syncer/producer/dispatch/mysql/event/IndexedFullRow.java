package com.github.zzt93.syncer.producer.dispatch.mysql.event;

import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zzt
 */
public class IndexedFullRow {

  private final Serializable[] now;
  private Serializable[] before;

  public IndexedFullRow(Serializable[] now) {
    this.now = now;
  }

  public IndexedFullRow setBefore(Serializable[] before) {
    this.before = before;
    return this;
  }

  NamedFullRow toNamed(List<Integer> interestedAndPkIndex, Map<Integer, String> indexToName) {
    int size = interestedAndPkIndex.size();
    HashMap<String, Object> now = Maps.newHashMapWithExpectedSize(size);
    for (Integer i : interestedAndPkIndex) {
      // TODO 2019/3/29 changed schema problem
      now.put(indexToName.get(i), this.now[i]);
    }
    HashMap<String, Object> before = null;
    if (this.before != null) {
      before = Maps.newHashMapWithExpectedSize(size);
      for (Integer i : interestedAndPkIndex) {
        before.put(indexToName.get(i), this.before[i]);
      }
    }
    return new NamedFullRow(now).setBeforeFull(before);
  }

  public String length() {
    return "IndexedFullRow{" +
        "now[0," + now.length +
        "), before[0," + (before != null ? before.length : 0) +
        ")}";
  }
}
