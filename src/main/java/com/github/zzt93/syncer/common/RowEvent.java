package com.github.zzt93.syncer.common;


import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import java.util.HashMap;
import java.util.List;

/**
 * @author zzt
 */
public abstract class RowEvent {

  private final Event tableMap;
  private HashMap<Integer, Object> data = new HashMap<>();

  public RowEvent(Event tableMap) {
    this.tableMap = tableMap;
  }

  public TableMapEventData getTableMap() {
    return tableMap.getData();
  }

  public Object put(Integer key, Object value) {
    return data.put(key, value);
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
}
