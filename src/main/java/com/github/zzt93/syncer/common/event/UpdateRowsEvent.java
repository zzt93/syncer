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

/**
 * Created by zzt on 9/14/17. <p>
 * <h3>
 * <a href="https://dev.mysql.com/doc/internals/en/event-data-for-specific-event-types.html">The format of update rows log event</a></h3>
 *
 * <ul>
 *   <li>included columns before: Bit-field indicating whether each column is used, one bit per column</li>
 *   <li> Bit-field indicating whether each column is used in the UPDATE_ROWS_LOG_EVENT after-image</li>
 * </ul>
 */
public class UpdateRowsEvent extends RowsEvent {

  public UpdateRowsEvent(Event tableMap, UpdateRowsEventData updateRowsEventData,
      Map<Integer, String> indexToName) {
    super(tableMap, indexToName);
    BitSet includedColumns = updateRowsEventData.getIncludedColumns();
    List<Entry<Serializable[], Serializable[]>> rows = updateRowsEventData.getRows();
    for (Entry<Serializable[], Serializable[]> row : rows) {
      HashMap<Integer, Object> map = new HashMap<>();
      Serializable[] after = row.getValue();
      for (int i = 0; i < after.length; i++) {
        if (includedColumns.get(i)) {
          map.put(i + 1, after[i]);
        }
      }
      addRow(map);
    }
  }

  @Override
  public EventType type() {
    return EventType.UPDATE_ROWS;
  }
}
