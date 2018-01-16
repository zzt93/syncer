package com.github.zzt93.syncer.producer.dispatch.event;

import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;

/**
 * Created by zzt on 9/14/17. <p> <h3></h3>
 */
public class WriteRowsEvent  {


  public static List<HashMap<Integer, Object>> getIndexedRows(WriteRowsEventData writeRowsEventData){
    List<HashMap<Integer, Object>> res = new ArrayList<>();
    BitSet includedColumns = writeRowsEventData.getIncludedColumns();
    List<Serializable[]> rows = writeRowsEventData.getRows();
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
