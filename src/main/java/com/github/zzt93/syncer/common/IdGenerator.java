package com.github.zzt93.syncer.common;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo;

/**
 * @author zzt
 */
public class IdGenerator {

  public static final String EID = "eid";
  private static final String SEP  = "/";

  public static String fromEvent(Event[] event, String binlogFileName) {
    EventHeaderV4 tableMap = event[0].getHeader();
    EventHeaderV4 second = event[1].getHeader();
    return second.getServerId() + SEP + binlogFileName + SEP + tableMap
        .getPosition() + SEP + second.getEventType();
  }

  public static String fromEventId(String eventId, int ordinal) {
    return eventId + SEP + ordinal;
  }

  public static BinlogInfo fromDataId(String dataId) {
    String[] split = dataId.split(SEP);
    if (split.length == 5) {
      return new BinlogInfo(split[1], Long.parseLong(split[2]));
    }
    throw new IllegalArgumentException(dataId);
  }

}
