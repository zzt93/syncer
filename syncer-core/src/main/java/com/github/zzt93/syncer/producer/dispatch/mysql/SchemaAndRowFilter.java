package com.github.zzt93.syncer.producer.dispatch.mysql;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.producer.dispatch.mysql.event.IndexedFullRow;
import com.github.zzt93.syncer.producer.dispatch.mysql.event.NamedFullRow;
import com.github.zzt93.syncer.producer.dispatch.mysql.event.RowsEvent;
import com.github.zzt93.syncer.producer.input.mysql.meta.ConsumerSchemaMeta;
import com.github.zzt93.syncer.producer.input.mysql.meta.TableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author zzt
 */
public class SchemaAndRowFilter {

  private final ConsumerSchemaMeta consumerSchemaMeta;
  private final Logger logger = LoggerFactory.getLogger(SchemaAndRowFilter.class);
  private boolean onlyUpdated;

  SchemaAndRowFilter(ConsumerSchemaMeta consumerSchemaMeta, boolean onlyUpdated) {
    this.consumerSchemaMeta = consumerSchemaMeta;
    this.onlyUpdated = onlyUpdated;
  }

  SyncData[] decide(SimpleEventType type, String eventId, Event... e) {
    TableMapEventData tableMap = e[0].getData();
    TableMeta table = consumerSchemaMeta.findTable(tableMap.getDatabase(), tableMap.getTable());
    if (table == null) {
      return null;
    }

    List<IndexedFullRow> indexedRow = RowsEvent
        .getIndexedRows(type, e[1].getData(), table.getPrimaryKeys());
    List<NamedFullRow> namedRow = RowsEvent
        .getNamedRows(indexedRow, table.getInterestedAndPkIndex(), table.getInterestedAndPkIndexToName());
    String primaryKey = RowsEvent.getPrimaryKey(table.getInterestedAndPkIndexToName(), table.getPrimaryKeys());
    SyncData[] res = new SyncData[namedRow.size()];
    for (int i = 0; i < res.length; i++) {
      NamedFullRow row = namedRow.get(i);
      if (onlyUpdated && type == SimpleEventType.UPDATE && row.getUpdated().isEmpty()) {
        // TODO 2019/3/20 change to debug when test finish
        logger.info("Discard {} because [{}]", eventId, row);
        assert i == 0;
        return null;
      }
      Object pk = row.get(primaryKey);
      if (!table.isInterestedPK()) {
        row.remove(primaryKey);
      }
      res[i] = new SyncData(eventId, i, type, tableMap.getDatabase(), tableMap.getTable(), primaryKey, pk, row);
    }
    return res;
  }


}
