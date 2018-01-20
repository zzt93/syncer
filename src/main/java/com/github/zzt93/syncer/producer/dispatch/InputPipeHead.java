package com.github.zzt93.syncer.producer.dispatch;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.producer.dispatch.event.RowsEvent;
import com.github.zzt93.syncer.producer.input.mysql.meta.ConnectionSchemaMeta;
import com.github.zzt93.syncer.producer.input.mysql.meta.TableMeta;
import java.util.HashMap;
import java.util.List;

/**
 * @author zzt
 */
public class InputPipeHead {

  private final ConnectionSchemaMeta connectionSchemaMeta;

  public InputPipeHead(ConnectionSchemaMeta connectionSchemaMeta) {
    this.connectionSchemaMeta = connectionSchemaMeta;
  }

  public SyncData[] decide(String eventId, Event... e) {
    TableMapEventData event = e[0].getData();
    TableMeta table = connectionSchemaMeta.findTable(event.getDatabase(), event.getTable());
    if (table == null) {
      return null;
    }

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
      res[i] = new SyncData(eventId, i, tableMap, primaryKey,
          namedRow.get(i), eventType);
    }
    return res;
  }


}
