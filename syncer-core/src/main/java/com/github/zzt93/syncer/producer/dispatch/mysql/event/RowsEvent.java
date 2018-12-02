package com.github.zzt93.syncer.producer.dispatch.mysql.event;


import com.github.shyiko.mysql.binlog.event.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.Serializable;
import java.util.*;

/**
 * <a href="https://dev.mysql.com/doc/internals/en/binlog-row-image.html">binlog row image
 * format</a>
 *
 * @author zzt
 */
public abstract class RowsEvent {

  private static final Logger logger = LoggerFactory.getLogger(RowsEvent.class);

  public static boolean filterData(List<HashMap<Integer, Object>> indexedRow,
      List<Integer> interestedAndPkIndex, EventType eventType) {
    Assert.isTrue(!indexedRow.isEmpty(), "Assertion Failure: no row to filter");
    List<HashMap<Integer, Object>> tmp = new LinkedList<>();
    for (HashMap<Integer, Object> row : indexedRow) {
      HashMap<Integer, Object> map = new HashMap<>();
      for (Integer integer : interestedAndPkIndex) {
        if (row.containsKey(integer)) {
          map.put(integer, row.get(integer));
        }
      }
      if (map.size() > 1 || (map.size() == 1 && eventType != EventType.UPDATE_ROWS)) {
        tmp.add(map);
      } else {
        Assert.isTrue(!map.isEmpty(), "Assertion Failure: should at least has primary key");
        // discard event which only has id && event type is UPDATE_ROWS
        logger.debug("Discard {} for {}", row, eventType);
      }
    }
    indexedRow.clear();
    indexedRow.addAll(tmp);
    return !indexedRow.isEmpty();
  }

  public static List<HashMap<String, Object>> getNamedRows(
      List<HashMap<Integer, Object>> indexedRow,
      Map<Integer, String> indexToName) {
    List<HashMap<String, Object>> res = new ArrayList<>(indexedRow.size());
    for (HashMap<Integer, Object> row : indexedRow) {
      HashMap<String, Object> map = new HashMap<>();
      for (Integer index : row.keySet()) {
        map.put(indexToName.get(index), row.get(index));
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
        WriteRowsEventData write = (WriteRowsEventData) data;
        return getIndexedRows(write.getRows(), write.getIncludedColumns());
      case DELETE_ROWS:
        DeleteRowsEventData delete = (DeleteRowsEventData) data;
        return getIndexedRows(delete.getRows(), delete.getIncludedColumns());
      default:
        throw new IllegalArgumentException("Unsupported event type");
    }
  }

  private static List<HashMap<Integer, Object>> getIndexedRows(List<Serializable[]> rows,
      BitSet includedColumns) {
    List<HashMap<Integer, Object>> res = new LinkedList<>();
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
