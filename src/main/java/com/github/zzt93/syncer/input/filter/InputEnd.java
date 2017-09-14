package com.github.zzt93.syncer.input.filter;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.event.RowEvent;
import com.github.zzt93.syncer.filter.Filter;

/**
 * @author zzt
 */
public class InputEnd implements Filter<RowEvent, SyncData> {

  @Override
  public SyncData decide(RowEvent e) {
    return new SyncData(e);
  }
}
