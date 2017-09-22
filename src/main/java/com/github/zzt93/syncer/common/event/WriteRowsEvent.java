package com.github.zzt93.syncer.common.event;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zzt on 9/14/17. <p> <h3></h3>
 */
public class WriteRowsEvent extends RowsEvent {


  public WriteRowsEvent(Event tableMap, WriteRowsEventData writeRowsEventData,
      Map<Integer, String> indexToName) {
    super(tableMap, indexToName);
    BitSet includedColumns = writeRowsEventData.getIncludedColumns();
    List<Serializable[]> rows = writeRowsEventData.getRows();
    for (Serializable[] row : rows) {
      HashMap<Integer, Object> map = new HashMap<>();
      for (int i = 0; i < row.length; i++) {
        if (includedColumns.get(i)) {
          map.put(i + 1, row[i]);
        }
      }
      addRow(map);
    }
  }

  @Override
  public EventType type() {
    return EventType.WRITE_ROWS;
  }
}
