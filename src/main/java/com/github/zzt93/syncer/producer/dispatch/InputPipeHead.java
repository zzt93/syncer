package com.github.zzt93.syncer.producer.dispatch;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.zzt93.syncer.common.ConnectionSchemaMeta;
import com.github.zzt93.syncer.common.Filter;
import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.TableMeta;
import com.github.zzt93.syncer.common.event.IdGenerator;
import com.github.zzt93.syncer.common.event.RowsEvent;
import java.util.HashMap;
import java.util.List;

/**
 * @author zzt
 */
public class InputPipeHead implements Filter<Event[], SyncData[]> {

  private final ConnectionSchemaMeta connectionSchemaMeta;

  public InputPipeHead(ConnectionSchemaMeta connectionSchemaMeta) {
    this.connectionSchemaMeta = connectionSchemaMeta;
  }

  @Override
  public SyncData[] decide(Event... e) {
    TableMapEventData event = e[0].getData();
    TableMeta table = connectionSchemaMeta.findTable(event.getDatabase(), event.getTable());
    if (table == null) {
      return null;
    }
    String eventId = IdGenerator.fromEvent(e[1]);

    TableMapEventData tableMap = e[0].getData();
    EventType eventType = e[1].getHeader().getEventType();
    List<HashMap<Integer, Object>> indexedRow = RowsEvent
        .getIndexedRows(eventType, e[1].getData(), table.getPrimaryKeys());
    boolean filtered = RowsEvent.filterData(indexedRow, table.getInterestedColIndex());
    if (!filtered) {
      return null;
    }

    List<HashMap<String, Object>> namedRow = RowsEvent
        .getNamedRows(indexedRow, table.getIndexToName());
    String primaryKey = RowsEvent.getPrimaryKey(table.getIndexToName(), table.getPrimaryKeys());
    SyncData[] res = new SyncData[namedRow.size()];
    for (int i = 0; i < res.length; i++) {
      res[i] = new SyncData(eventId, tableMap, primaryKey,
          namedRow.get(i), eventType);
    }
    return res;
  }


}
