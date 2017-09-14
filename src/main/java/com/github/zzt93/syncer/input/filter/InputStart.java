package com.github.zzt93.syncer.input.filter;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.zzt93.syncer.common.DeleteRowEvent;
import com.github.zzt93.syncer.common.RowEvent;
import com.github.zzt93.syncer.common.UpdateRowEvent;
import com.github.zzt93.syncer.common.WriteRowEvent;
import com.github.zzt93.syncer.filter.Filter;
import org.springframework.util.Assert;

/**
 * @author zzt
 */
public class InputStart implements Filter<Event[], RowEvent> {

  @Override
  public RowEvent decide(Event... e) {
    Assert.isTrue(e[0].getHeader().getEventType() == EventType.TABLE_MAP, "[Assertion failed] ");
    switch (e[1].getHeader().getEventType()) {
      case WRITE_ROWS:
        return new WriteRowEvent(e[0], e[1].getData());
      case UPDATE_ROWS:
        return new UpdateRowEvent(e[0], e[1].getData());
      case DELETE_ROWS:
        return new DeleteRowEvent(e[0], e[1].getData());
      default:
        throw new IllegalArgumentException();
    }
  }
}
