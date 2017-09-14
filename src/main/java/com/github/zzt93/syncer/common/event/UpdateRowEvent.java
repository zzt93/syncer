package com.github.zzt93.syncer.common.event;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import java.io.Serializable;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Created by zzt on 9/14/17. <p> <h3></h3>
 */
public class UpdateRowEvent extends RowEvent {

  public UpdateRowEvent(Event tableMap, UpdateRowsEventData updateRowsEventData,
      Map<Integer, String> indexToName) {
    super(tableMap, indexToName);
    BitSet includedColumns = updateRowsEventData.getIncludedColumns();
    List<Entry<Serializable[], Serializable[]>> rows = updateRowsEventData.getRows();
    int c = 0;
    for (int i = 0; i < includedColumns.length(); i++) {
      if (includedColumns.get(i)) {
        put(i, rows.get(c++).getValue());
      }
    }
  }

}
