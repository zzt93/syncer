package com.github.zzt93.syncer.producer.dispatch.mysql.event;

import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

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

  static List<HashMap<Integer, Object>> getIndexedRows(
      UpdateRowsEventData updateRowsEventData, Set<Integer> primaryKeys){
    List<HashMap<Integer, Object>> res = new ArrayList<>();
    BitSet includedColumns = updateRowsEventData.getIncludedColumns();
    // TODO 17/10/10 may support different binlog row image, only 'full' now
    // If support 'minimal' format:
    // - it will only keep columns needed to identify rows (id) & updated fields, but not partition key in DRDS
    RowUpdateImageMapper mapper = new FullRowUpdateImageMapper(primaryKeys, includedColumns);
    List<Entry<Serializable[], Serializable[]>> rows = updateRowsEventData.getRows();
    for (Entry<Serializable[], Serializable[]> row : rows) {
      HashMap<Integer, Object> map = mapper.map(row);
      res.add(map);
    }
    return res;
  }

}
