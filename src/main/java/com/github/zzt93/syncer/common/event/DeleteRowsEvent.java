package com.github.zzt93.syncer.common.event;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by zzt on 9/14/17. <p> <h3></h3>
 */
public class DeleteRowsEvent extends RowsEvent {

  public DeleteRowsEvent(Event tableMap, DeleteRowsEventData deleteRowsEventData,
      Map<Integer, String> indexToName, Set<Integer> primaryKeys) {
    super(tableMap, indexToName, primaryKeys);
    // TODO 17/10/25 only keep id
    BitSet includedColumns = deleteRowsEventData.getIncludedColumns();
    List<Serializable[]> rows = deleteRowsEventData.getRows();
    for (Serializable[] row : rows) {
      HashMap<Integer, Object> map = new HashMap<>();
      for (int i = 0; i < row.length; i++) {
        if (includedColumns.get(i)) {
          map.put(i, row[i]);
        }
      }
      addRow(map);
    }
  }


  @Override
  public EventType operationType() {
    return EventType.DELETE_ROWS;
  }
}
