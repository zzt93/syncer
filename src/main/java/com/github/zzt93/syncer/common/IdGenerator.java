package com.github.zzt93.syncer.common;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.zzt93.syncer.producer.input.connect.BinlogInfo;

/**
 * @author zzt
 */
public class IdGenerator {

  public static final String EID = "eid";
  private static final String SEP  = "_";

  public static String fromEvent(Event event, String binlogFileName) {
    EventHeaderV4 header = event.getHeader();
    return header.getServerId() + SEP + binlogFileName + SEP + header
        .getPosition() + SEP + header.getEventType();
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
