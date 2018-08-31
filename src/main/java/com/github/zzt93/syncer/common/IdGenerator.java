package com.github.zzt93.syncer.common;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.zzt93.syncer.producer.input.mongo.DocTimestamp;
import com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo;
import org.bson.BsonTimestamp;
import org.bson.Document;

/**
 * @author zzt
 */
public class IdGenerator {

  public static final String EID = "eid";
  private static final int COMMON_LEN = 40;
  private static final String SEP = "/";

  /**
   * <a href="https://github.com/shyiko/mysql-binlog-connector-java/issues/200">binlog table
   * map</a>
   */
  public static String fromEvent(Event[] event, String binlogFileName) {
    EventHeaderV4 tableMap = event[0].getHeader();
    EventHeaderV4 second = event[1].getHeader();
    // have to remember table map event for restart
    // have to add following data event for unique id
    return new StringBuilder(COMMON_LEN)
        .append(binlogFileName).append(SEP)
        .append(tableMap.getPosition()).append(SEP)
        .append(second.getPosition() - tableMap.getPosition()).append(SEP)
        .append(getEventTypeAbbr(second)).toString();
  }

  private static String getEventTypeAbbr(EventHeaderV4 second) {
    switch (second.getEventType()) {
      case UPDATE_ROWS:
        return "u";
      case WRITE_ROWS:
        return "w";
      case DELETE_ROWS:
        return "d";
      default:
        throw new IllegalArgumentException("Unsupported event type");
    }
  }

  public static String fromEventId(String eventId, int ordinal) {
    return new StringBuilder(COMMON_LEN).append(eventId).append(SEP).append(ordinal).toString();
  }

  public static String fromEventId(String eventId, int ordinal, int offset) {
    return new StringBuilder(COMMON_LEN).append(eventId).append(SEP).append(ordinal).append(SEP)
        .append(offset).toString();
  }

  public static BinlogInfo fromDataId(String dataId) {
    String[] split = dataId.split(SEP);
    if (split.length == 5 || split.length == 6) {
      return new BinlogInfo(split[0], Long.parseLong(split[1]));
    }
    throw new IllegalArgumentException(dataId);
  }

  public static String fromDocument(Document document) {
    BsonTimestamp timestamp = (BsonTimestamp) document.get("ts");
    return timestamp.getTime() + SEP + timestamp.getInc();
  }

  public static DocTimestamp fromMongoDataId(String dataId) {
    String[] split = dataId.split(SEP);
    if (split.length == 3) {
      return new DocTimestamp(
          new BsonTimestamp(Integer.parseInt(split[0]), Integer.parseInt(split[1])));
    }
    throw new IllegalArgumentException(dataId);
  }

  public enum Offset {
    DUP(), CLONE()
  }

  public static int maxIdLen() {
    return COMMON_LEN * 2;
  }

}
