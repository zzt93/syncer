package com.github.zzt93.syncer.common;


import com.github.shyiko.mysql.binlog.event.Event;

/**
 * @author zzt
 */
public class MysqlRowEvent {

  private final Event tableMap;
  private final Event rowEvent;

  public MysqlRowEvent(Event tableMap, Event rowEvent) {
    this.tableMap = tableMap;
    this.rowEvent = rowEvent;
  }

  public Event getTableMap() {
    return tableMap;
  }

  public Event getRowEvent() {
    return rowEvent;
  }
}
