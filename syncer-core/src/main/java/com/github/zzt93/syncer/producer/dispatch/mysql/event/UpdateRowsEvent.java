package com.github.zzt93.syncer.producer.dispatch.mysql.event;

import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

/**
 * Created by zzt on 9/14/17.
 * <p>
 *
 * <h3> <a href="https://dev.mysql.com/doc/internals/en/event-data-for-specific-event-types.html">The
 * format of update rows log event</a></h3>
 *
 * <ul> <li>before update row image & bit field indicating presence</li> <li>after update row image
 * & bit field indicating presence</li> </ul>
 *
 * <p> <a href="https://dev.mysql.com/doc/refman/5.7/en/replication-options-binary-log.html#sysvar_binlog_row_image">
 *   The binlog row image format</a>: For now, only support 'full' now </p>
 */
public class UpdateRowsEvent  {

  static List<IndexedFullRow> getIndexedRows(UpdateRowsEventData updateRowsEventData){
    List<IndexedFullRow> res = new ArrayList<>();
    // TODO 17/10/10 may support different binlog row image, only 'full' now
    // If support 'minimal' format:
    // - it will only keep columns needed to identify rows (id) & updated fields, but not partition key in DRDS
    // - it will disable upsert related function of ES output channel
    List<Entry<Serializable[], Serializable[]>> rows = updateRowsEventData.getRows();
    for (Entry<Serializable[], Serializable[]> row : rows) {
      res.add(new IndexedFullRow(row.getValue()).setBefore(row.getKey()));
    }
    return res;
  }

}
