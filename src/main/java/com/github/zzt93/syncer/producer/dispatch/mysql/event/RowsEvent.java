package com.github.zzt93.syncer.producer.dispatch.mysql.event;


import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.util.Assert;

/**
 * <a href="https://dev.mysql.com/doc/internals/en/binlog-row-image.html">binlog row image
 * format</a>
 *
 * @author zzt
 */
public abstract class RowsEvent {

  public static boolean filterData(List<HashMap<Integer, Object>> indexedRow,
      List<Integer> interested) {
    Assert.isTrue(!indexedRow.isEmpty(), "Assertion Failure: no row to filter");
    List<HashMap<Integer, Object>> tmp = new ArrayList<>();
    for (HashMap<Integer, Object> row : indexedRow) {
      HashMap<Integer, Object> map = new HashMap<>();
      for (Integer integer : interested) {
        if (row.containsKey(integer)) {
          map.put(integer, row.get(integer));
        }
      }
      if (!map.isEmpty()) {
        tmp.add(map);
      }
    }
    indexedRow.clear();
    indexedRow.addAll(tmp);
    return !indexedRow.isEmpty();
  }

  public static List<HashMap<String, Object>> getNamedRows(
      List<HashMap<Integer, Object>> indexedRow,
      Map<Integer, String> indexToName) {
    List<HashMap<String, Object>> res = new ArrayList<>();
    for (HashMap<Integer, Object> row : indexedRow) {
      HashMap<String, Object> map = new HashMap<>();
      for (Integer integer : row.keySet()) {
        map.put(indexToName.get(integer), row.get(integer));
      }
      res.add(map);
    }
    return res;
  }

  public static String getPrimaryKey(Map<Integer, String> indexToName, Set<Integer> primaryKeys) {
    Iterator<Integer> iterator = primaryKeys.iterator();
    Integer key = iterator.next();
    return indexToName.get(key);
  }

  public static List<HashMap<Integer, Object>> getIndexedRows(EventType eventType, EventData data,
      Set<Integer> primaryKeys) {
    switch (eventType) {
      case UPDATE_ROWS:
        return UpdateRowsEvent.getIndexedRows((UpdateRowsEventData) data, primaryKeys);
      case WRITE_ROWS:
        return WriteRowsEvent.getIndexedRows((WriteRowsEventData) data);
      case DELETE_ROWS:
        return DeleteRowsEvent.getIndexedRows((DeleteRowsEventData) data);
      default:
        throw new IllegalArgumentException("Unsupported event type");
    }
  }
}
