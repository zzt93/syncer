package com.github.zzt93.syncer.common.event;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;

/**
 * @author zzt
 */
public class IdGenerator {

  public static String fromEvent(Event event) {
    EventHeaderV4 header = event.getHeader();
    return header.getServerId()+"."+header.getEventType()+"."+header.getPosition();
  }

}
