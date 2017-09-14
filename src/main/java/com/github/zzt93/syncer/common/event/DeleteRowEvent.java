package com.github.zzt93.syncer.common.event;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import java.io.Serializable;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * Created by zzt on 9/14/17. <p> <h3></h3>
 */
public class DeleteRowEvent extends RowEvent {

  public DeleteRowEvent(Event tableMap, DeleteRowsEventData deleteRowsEventData,
      Map<Integer, String> indexToName) {
    super(tableMap, indexToName);
    BitSet includedColumns = deleteRowsEventData.getIncludedColumns();
    List<Serializable[]> rows = deleteRowsEventData.getRows();
    int c = 0;
    for (int i = 0; i < includedColumns.length(); i++) {
      if (includedColumns.get(i)) {
        put(i, rows.get(c++));
      }
    }
  }


}
