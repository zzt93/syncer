package com.github.zzt93.syncer.producer.dispatch.mysql;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.producer.dispatch.mysql.event.RowsEvent;
import com.github.zzt93.syncer.producer.input.mysql.meta.ConnectionSchemaMeta;
import com.github.zzt93.syncer.producer.input.mysql.meta.TableMeta;
import java.util.HashMap;
import java.util.List;

/**
 * @author zzt
 */
public class SchemaAndRowFilter {

  private final ConnectionSchemaMeta connectionSchemaMeta;

  public SchemaAndRowFilter(ConnectionSchemaMeta connectionSchemaMeta) {
    this.connectionSchemaMeta = connectionSchemaMeta;
  }

  public SyncData[] decide(String eventId, Event... e) {
    TableMapEventData tableMap = e[0].getData();
    TableMeta table = connectionSchemaMeta.findTable(tableMap.getDatabase(), tableMap.getTable());
    if (table == null) {
      return null;
    }

    EventType eventType = e[1].getHeader().getEventType();
    List<HashMap<Integer, Object>> indexedRow = RowsEvent
        .getIndexedRows(eventType, e[1].getData(), table.getPrimaryKeys());
    boolean hasMore = RowsEvent.filterData(indexedRow, table.getInterestedColIndex(), eventType);
    if (!hasMore) {
      return null;
    }

    List<HashMap<String, Object>> namedRow = RowsEvent
        .getNamedRows(indexedRow, table.getIndexToName());
    String primaryKey = RowsEvent.getPrimaryKey(table.getIndexToName(), table.getPrimaryKeys());
    SyncData[] res = new SyncData[namedRow.size()];
    for (int i = 0; i < res.length; i++) {
      HashMap<String, Object> row = namedRow.get(i);
      res[i] = new SyncData(eventId, i, tableMap.getDatabase(), tableMap.getTable(), primaryKey,
          row.get(primaryKey), row, eventType);
      if (!table.isInterestedPK()) {
        row.remove(primaryKey);
      }
    }
    return res;
  }


}
