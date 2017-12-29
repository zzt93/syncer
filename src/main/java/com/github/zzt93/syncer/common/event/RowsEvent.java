package com.github.zzt93.syncer.common.event;


import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
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

  public static final String EID = "eid";
  private final String eventId;

  private final Event tableMap;
  private final Map<Integer, String> indexToName;
  private List<HashMap<Integer, Object>> rows = new ArrayList<>();
  private Set<Integer> primaryKeys;

  public RowsEvent(String eventId, Event tableMap, Map<Integer, String> indexToName,
      Set<Integer> primaryKeys) {
    this.eventId = eventId;
    this.tableMap = tableMap;
    this.indexToName = indexToName;
    this.primaryKeys = primaryKeys;
  }

  public TableMapEventData getTableMap() {
    return tableMap.getData();
  }

  void addRow(HashMap<Integer, Object> row) {
    rows.add(row);
  }

  public boolean filterData(List<Integer> interested) {
    Assert.isTrue(!rows.isEmpty(), "Assertion Failure: no row to filter");
    List<HashMap<Integer, Object>> tmp = new ArrayList<>();
    for (HashMap<Integer, Object> row : rows) {
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
    rows = tmp;
    return !rows.isEmpty();
  }

  public List<HashMap<String, Object>> getRows() {
    List<HashMap<String, Object>> res = new ArrayList<>();
    for (HashMap<Integer, Object> row : rows) {
      HashMap<String, Object> map = new HashMap<>();
      for (Integer integer : row.keySet()) {
        map.put(indexToName.get(integer), row.get(integer));
      }
      res.add(map);
    }
    return res;
  }

  public String getPrimaryKey() {
    Iterator<Integer> iterator = primaryKeys.iterator();
    Integer key = iterator.next();
    return indexToName.get(key);
  }

  public String getEventId() {
    return eventId;
  }

  @Override
  public String toString() {
    return "RowsEvent{" +
        "tableMap=" + tableMap +
        ", indexToName=" + indexToName +
        ", rows=" + rows +
        ", primaryKeys=" + primaryKeys +
        '}';
  }

  public abstract EventType operationType();
}
