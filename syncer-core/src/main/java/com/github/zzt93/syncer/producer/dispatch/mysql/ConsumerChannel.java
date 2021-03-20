package com.github.zzt93.syncer.producer.dispatch.mysql;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.zzt93.syncer.common.Filter.FilterRes;
import com.github.zzt93.syncer.common.data.BinlogDataId;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.producer.dispatch.mysql.event.IndexedFullRow;
import com.github.zzt93.syncer.producer.dispatch.mysql.event.NamedFullRow;
import com.github.zzt93.syncer.producer.dispatch.mysql.event.RowsEvent;
import com.github.zzt93.syncer.producer.input.mysql.AlterMeta;
import com.github.zzt93.syncer.producer.input.mysql.meta.ConsumerSchemaMeta;
import com.github.zzt93.syncer.producer.input.mysql.meta.TableMeta;
import com.github.zzt93.syncer.producer.output.ProducerSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * @author zzt
 */
public class ConsumerChannel {

  private final Logger logger = LoggerFactory.getLogger(ConsumerChannel.class);
  private final ProducerSink producerSink;
  private final ConsumerSchemaMeta consumerSchemaMeta;
  private final boolean onlyUpdated;

  ConsumerChannel(ConsumerSchemaMeta consumerSchemaMeta, ProducerSink producerSink, boolean onlyUpdated) {
    this.consumerSchemaMeta = consumerSchemaMeta;
    this.onlyUpdated = onlyUpdated;
    this.producerSink = producerSink;
  }

  FilterRes decide(SimpleEventType simpleEventType, BinlogDataId dataId, Event[] events) {
    if (logger.isTraceEnabled()) {
      logger.trace("Receive binlog event: {}", Arrays.toString(events));
    }
    SyncData[] aim = toSyncData(simpleEventType, dataId, events[0], events[1]);
    if (aim == null) { // not interested in this database+table
      return FilterRes.DENY;
    }

    if (hold(aim)) {
      return FilterRes.ACCEPT;
    }

    boolean output = producerSink.output(aim);
    return output ? FilterRes.ACCEPT : FilterRes.DENY;
  }

  private boolean hold(SyncData[] aim) {
    // TODO: 2021/3/21
    return false;
  }

  public boolean output(SyncData[] data) {
    return producerSink.output(data);
  }

  private SyncData[] toSyncData(SimpleEventType type, BinlogDataId dataId, Event... e) {
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
    boolean hasData = false;
    for (int i = 0; i < res.length; i++) {
      NamedFullRow row = namedRow.get(i);
      if (onlyUpdated && type == SimpleEventType.UPDATE && row.getUpdated().isEmpty()) {
        logger.debug("Discard {} because [{}]", dataId.eventId(), row);
        // even though in one update event, multiple rows can have different updated column,
        // so we can only skip one by one
        // e.g. we listening 'name1' but not 'name2' and the following update will make all updates in a single event
        // UPDATE relation
        // SET name1 = CASE WHEN userid1 = 3 THEN 'jack' ELSE name1 END,
        //     name2 = CASE WHEN userid2 = 3 THEN 'jack' ELSE name2 END
        continue;
      }
      Object pk = row.get(primaryKey);
      if (!table.isInterestedPK()) {
        row.remove(primaryKey);
      }
      hasData = true;
      res[i] = new SyncData(dataId.copyAndSetOrdinal(i), type, tableMap.getDatabase(), tableMap.getTable(), primaryKey, pk, row);
    }
    if (hasData) {
      return res;
    }
    return null;
  }

  void updateSchemaMeta(AlterMeta alterMeta, TableMeta full) {
    consumerSchemaMeta.updateSchemaMeta(alterMeta, full);
  }

  boolean interestedSchemaMeta(AlterMeta alterMeta) {
    return consumerSchemaMeta.findTable(alterMeta.getSchema(), alterMeta.getTable()) != null;
  }

}
