package com.github.zzt93.syncer.common.event;


import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zzt
 */
public abstract class RowEvent {

  private final Event tableMap;
  private final Map<Integer, String> indexToName;
  private HashMap<Integer, Object> data = new HashMap<>();

  public RowEvent(Event tableMap, Map<Integer, String> indexToName) {
    this.tableMap = tableMap;
    this.indexToName = indexToName;
  }

  public TableMapEventData getTableMap() {
    return tableMap.getData();
  }

  void put(Integer key, Object value) {
    data.put(key, value);
  }

  public boolean filterData(List<Integer> index) {
    HashMap<Integer, Object> tmp = new HashMap<>(data);
    for (Integer integer : index) {
      if (data.containsKey(integer)) {
        tmp.put(integer, data.get(integer));
      }
    }
    data = tmp;
    return !data.isEmpty();
  }

  public HashMap<String, Object> getData() {
    HashMap<String, Object> res = new HashMap<>();
    for (Integer integer : data.keySet()) {
      res.put(indexToName.get(integer), data.get(integer));
    }
    return res;
  }

  public abstract EventType type();
}
