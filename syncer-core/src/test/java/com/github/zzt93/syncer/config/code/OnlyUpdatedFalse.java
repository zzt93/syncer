package com.github.zzt93.syncer.config.code;

import com.github.zzt93.syncer.data.SyncData;
import com.github.zzt93.syncer.data.util.MethodFilter;

import java.util.List;

/**
 * @author zzt
 */
public class OnlyUpdatedFalse implements MethodFilter {

  @Override
  public void filter(List<SyncData> list) {
    SyncData sync = list.get(0);
    if (!sync.updated()) {
      list.clear();
      return;
    }
  }

}
