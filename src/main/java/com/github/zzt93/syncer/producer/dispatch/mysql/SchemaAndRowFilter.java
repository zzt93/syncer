package com.github.zzt93.syncer.producer.dispatch.mysql;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.producer.dispatch.mysql.event.RowsEvent;
import com.github.zzt93.syncer.producer.input.mysql.meta.ConsumerSchemaMeta;
import com.github.zzt93.syncer.producer.input.mysql.meta.TableMeta;
import java.util.HashMap;
import java.util.List;

/**
 * @author zzt
 */
public class SchemaAndRowFilter {

  private final ConsumerSchemaMeta consumerSchemaMeta;

  public SchemaAndRowFilter(ConsumerSchemaMeta consumerSchemaMeta) {
    this.consumerSchemaMeta = consumerSchemaMeta;
  }

  public SyncData[] decide(String eventId, Event... e) {
    TableMapEventData tableMap = e[0].getData();
    TableMeta table = consumerSchemaMeta.findTable(tableMap.getDatabase(), tableMap.getTable());
    if (table == null) {
      return null;
    }

    EventType eventType = e[1].getHeader().getEventType();
    List<HashMap<Integer, Object>> indexedRow = RowsEvent
        .getIndexedRows(eventType, e[1].getData(), table.getPrimaryKeys());
    boolean hasMore = RowsEvent.filterData(indexedRow, table.getInterestedAndPkIndex(), eventType);
    if (!hasMore) {
      return null;
    }

    List<HashMap<String, Object>> namedRow = RowsEvent
        .getNamedRows(indexedRow, table.getInterestedAndPkIndexToName());
    String primaryKey = RowsEvent.getPrimaryKey(table.getInterestedAndPkIndexToName(), table.getPrimaryKeys());
    SyncData[] res = new SyncData[namedRow.size()];
    for (int i = 0; i < res.length; i++) {
      HashMap<String, Object> row = namedRow.get(i);
      Object pk = row.get(primaryKey);
      if (!table.isInterestedPK()) {
        row.remove(primaryKey);
      }
      res[i] = new SyncData(eventId, i, tableMap.getDatabase(), tableMap.getTable(), primaryKey,
          pk, row, eventType);
    }
    return res;
  }


}
