package com.github.zzt93.syncer.input.filter;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.zzt93.syncer.common.MysqlRowEvent;
import com.github.zzt93.syncer.filter.Filter;

/**
 * @author zzt
 */
public class InputStart implements Filter<Event[], MysqlRowEvent> {

  @Override
  public MysqlRowEvent decide(Event... e) {
    return new MysqlRowEvent(e[0], e[1]);
  }
}
