package com.github.zzt93.syncer.common;


import com.github.shyiko.mysql.binlog.event.Event;

/**
 * @author zzt
 */
public class MysqlRowEvent {

  private Event tableMap;
  private Event rowEvent;

  public MysqlRowEvent(Event tableMap, Event rowEvent) {
    this.tableMap = tableMap;
    this.rowEvent = rowEvent;
  }


}
