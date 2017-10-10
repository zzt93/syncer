package com.github.zzt93.syncer.input.filter;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.zzt93.syncer.common.Filter;
import com.github.zzt93.syncer.common.SchemaMeta;
import com.github.zzt93.syncer.common.TableMeta;
import com.github.zzt93.syncer.common.event.DeleteRowsEvent;
import com.github.zzt93.syncer.common.event.RowsEvent;
import com.github.zzt93.syncer.common.event.UpdateRowsEvent;
import com.github.zzt93.syncer.common.event.WriteRowsEvent;

/**
 * @author zzt
 */
public class InputStart implements Filter<Event[], RowsEvent> {

  private final SchemaMeta schemaMeta;

  public InputStart(SchemaMeta schemaMeta) {
    this.schemaMeta = schemaMeta;
  }

  @Override
  public RowsEvent decide(Event... e) {
    TableMapEventData event = e[0].getData();
    TableMeta table = schemaMeta.findTable(event.getDatabase(), event.getTable());
    if (table == null) {
      return null;
    }
    switch (e[1].getHeader().getEventType()) {
      case WRITE_ROWS:
        return new WriteRowsEvent(e[0], e[1].getData(), table.getIndexToName());
      case UPDATE_ROWS:
        return new UpdateRowsEvent(e[0], e[1].getData(), table.getIndexToName(), table.getPrimaryKeys());
      case DELETE_ROWS:
        return new DeleteRowsEvent(e[0], e[1].getData(), table.getIndexToName());
      default:
        throw new IllegalArgumentException();
    }
  }
}
