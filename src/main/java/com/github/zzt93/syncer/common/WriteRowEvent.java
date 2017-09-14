package com.github.zzt93.syncer.common;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;

/**
 * Created by zzt on 9/14/17. <p> <h3></h3>
 */
public class WriteRowEvent extends RowEvent {


  public WriteRowEvent(Event tableMap, WriteRowsEventData writeRowsEventData) {
    super(tableMap);
    BitSet includedColumns = writeRowsEventData.getIncludedColumns();
    List<Serializable[]> rows = writeRowsEventData.getRows();
    int c = 0;
    for (int i = 0; i < includedColumns.length(); i++) {
      if (includedColumns.get(i)) {
        put(i, rows.get(c++));
      }
    }
  }

}
