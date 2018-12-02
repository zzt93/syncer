package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.consumer.filter.SimpleStatement;

import java.util.List;

/**
 * @author zzt
 */
public class Drop implements SimpleStatement {

  @Override
  public void filter(List<SyncData> e) {
    e.clear();
  }

}
