package com.github.zzt93.syncer.common;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.zzt93.syncer.common.data.SyncInitMeta;
import com.github.zzt93.syncer.producer.input.mongo.DocTimestamp;
import com.github.zzt93.syncer.producer.input.mongo.MongoMasterConnector;
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
        .append(second.getPosition() - tableMap.getPosition())
        .toString();
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
    if (split.length == 4
        || split.length == 5) { // for backward compatibility
      return BinlogInfo.withFilenameCheck(split[0], Long.parseLong(split[1]));
    }
    throw new IllegalArgumentException(dataId);
  }

  public static String fromDocument(Document document) {
    BsonTimestamp timestamp = (BsonTimestamp) document.get(MongoMasterConnector.TS);
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

  public static SyncInitMeta getSyncMeta(String dataId) {
    String[] split = dataId.split(SEP);
    if (split.length == 4
        || split.length == 5) { // for backward compatibility
      return new BinlogInfo(split[0], Long.parseLong(split[1]));
    }
    if (split.length == 3) {
      return new DocTimestamp(
          new BsonTimestamp(Integer.parseInt(split[0]), Integer.parseInt(split[1])));
    }
    throw new IllegalArgumentException(dataId);
  }

  public enum Offset {
    DUP(), CLONE()
  }

}
