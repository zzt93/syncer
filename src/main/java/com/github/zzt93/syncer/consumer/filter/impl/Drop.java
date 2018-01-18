package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.consumer.filter.ExprFilter;
import java.util.List;

/**
 * @author zzt
 */
public class Drop implements ExprFilter, IfBodyAction {

  @Override
  public Void decide(List<SyncData> e) {
    e.clear();
    return null;
  }

  @Override
  public Object execute(SyncData data) {
    return FilterRes.DENY;
  }
}
