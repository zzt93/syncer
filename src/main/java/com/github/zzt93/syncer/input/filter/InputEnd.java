package com.github.zzt93.syncer.input.filter;

import com.github.zzt93.syncer.common.Filter;
import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.event.RowEvent;

/**
 * @author zzt
 */
public class InputEnd implements Filter<RowEvent, SyncData> {

  @Override
  public SyncData decide(RowEvent e) {
    return new SyncData(e, e.type());
  }
}
