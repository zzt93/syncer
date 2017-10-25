package com.github.zzt93.syncer.common.event;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Created by zzt on 9/14/17. <p> <h3> <a href="https://dev.mysql.com/doc/internals/en/event-data-for-specific-event-types.html">The
 * format of update rows log event</a></h3>
 *
 * <ul> <li>before update row image & bit field indicating presence</li> <li>after update row image
 * & bit field indicating presence</li> </ul>
 *
 * <p> <a href="https://dev.mysql.com/doc/refman/5.7/en/replication-options-binary-log.html">The
 * binlog row image format</a>: For now, only support 'full' </p>
 */
public class UpdateRowsEvent extends RowsEvent {

  public UpdateRowsEvent(Event tableMap, UpdateRowsEventData updateRowsEventData,
      Map<Integer, String> indexToName, Set<Integer> primaryKeys) {
    super(tableMap, indexToName, primaryKeys);
    BitSet includedColumns = updateRowsEventData.getIncludedColumns();
    // TODO 17/10/10 may support different binlog row image, only 'full' now
    RowUpdateImageMapper mapper = new FullRowUpdateImageMapper(tableMap, indexToName, primaryKeys, includedColumns);
    List<Entry<Serializable[], Serializable[]>> rows = updateRowsEventData.getRows();
    for (Entry<Serializable[], Serializable[]> row : rows) {
      HashMap<Integer, Object> map = mapper.map(row);
      addRow(map);
    }
  }

  @Override
  public EventType operationType() {
    return EventType.UPDATE_ROWS;
  }
}
