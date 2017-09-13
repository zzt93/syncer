package com.github.zzt93.syncer.input.filter;

import com.github.zzt93.syncer.common.MysqlRowEvent;
import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.filter.Filter;

/**
 * @author zzt
 */
public class InputEnd implements Filter<MysqlRowEvent, SyncData> {

  @Override
  public SyncData decide(MysqlRowEvent e) {
    return new SyncData(e);
  }
}
