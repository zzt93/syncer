package com.github.zzt93.syncer.input.filter;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.zzt93.syncer.common.Filter;
import com.github.zzt93.syncer.common.SchemaMeta;
import com.github.zzt93.syncer.common.TableMeta;
import com.github.zzt93.syncer.common.event.DeleteRowEvent;
import com.github.zzt93.syncer.common.event.RowEvent;
import com.github.zzt93.syncer.common.event.UpdateRowEvent;
import com.github.zzt93.syncer.common.event.WriteRowEvent;
import org.springframework.util.Assert;

/**
 * @author zzt
 */
public class InputStart implements Filter<Event[], RowEvent> {

  private final SchemaMeta schemaMeta;

  public InputStart(SchemaMeta schemaMeta) {
    this.schemaMeta = schemaMeta;
  }

  @Override
  public RowEvent decide(Event... e) {
    Assert.isTrue(e[0].getHeader().getEventType() == EventType.TABLE_MAP, "[Assertion failed] ");
    TableMapEventData event = e[0].getData();
    TableMeta table = schemaMeta.findTable(event.getDatabase(), event.getTable());
    Assert.notNull(table, "Assertion Failure: fail to find the name: " + event);
    switch (e[1].getHeader().getEventType()) {
      case WRITE_ROWS:
        return new WriteRowEvent(e[0], e[1].getData(), table.getIndexToName());
      case UPDATE_ROWS:
        return new UpdateRowEvent(e[0], e[1].getData(), table.getIndexToName());
      case DELETE_ROWS:
        return new DeleteRowEvent(e[0], e[1].getData(), table.getIndexToName());
      default:
        throw new IllegalArgumentException();
    }
  }
}
