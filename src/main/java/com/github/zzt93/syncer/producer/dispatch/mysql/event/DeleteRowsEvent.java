package com.github.zzt93.syncer.producer.dispatch.mysql.event;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;

/**
 * Created by zzt on 9/14/17. <p> <h3></h3>
 */
public class DeleteRowsEvent {

  static List<HashMap<Integer, Object>> getIndexedRows(
      DeleteRowsEventData deleteRowsEventData) {
    List<HashMap<Integer, Object>> res = new ArrayList<>();
    BitSet includedColumns = deleteRowsEventData.getIncludedColumns();
    List<Serializable[]> rows = deleteRowsEventData.getRows();
    // keep non-null field: id, partition key & foreign key
    for (Serializable[] row : rows) {
      HashMap<Integer, Object> map = new HashMap<>();
      for (int i = 0; i < row.length; i++) {
        if (includedColumns.get(i)) {
          map.put(i, row[i]);
        }
      }
      res.add(map);
    }
    return res;
  }

}
