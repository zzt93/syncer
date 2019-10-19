package com.github.zzt93.syncer.common.data;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.zzt93.syncer.producer.input.mongo.DocTimestamp;
import com.github.zzt93.syncer.producer.input.mongo.MongoMasterConnector;
import com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo;
import org.bson.BsonTimestamp;
import org.bson.Document;

/**
 * @author zzt
 */
public interface DataId extends Comparable<DataId> {

  String SEP = "/";

  static DataId fromString(String dataId) {
    String[] split = dataId.split(SEP);
    if (split.length == 4
        || split.length == 5) {
      long tableMapPos = Long.parseLong(split[1]);
      BinlogDataId binlogDataId = new BinlogDataId(split[0], tableMapPos, Long.parseLong(split[2]) + tableMapPos).setOrdinal(Integer.parseInt(split[3]));
      if (split.length == 4) {
        return binlogDataId;
      }
      return binlogDataId.setOffset(Integer.parseInt(split[4]));
    }
    if (split.length == 2
        || split.length == 3) { // for backward compatibility
      return new MongoDataId(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
    }
    throw new IllegalArgumentException(dataId);
  }

  /**
   * <a href="https://github.com/shyiko/mysql-binlog-connector-java/issues/200">binlog table
   * map</a>
   */
  static BinlogDataId fromEvent(Event[] event, String binlogFileName) {
    EventHeaderV4 tableMap = event[0].getHeader();
    EventHeaderV4 second = event[1].getHeader();
    // have to remember table map event for restart
    // have to add data event position for unique id:
    // because one table map event may follow multiple data event
    return new BinlogDataId(binlogFileName, tableMap.getPosition(), second.getPosition());
  }

  static MongoDataId fromDocument(Document document) {
    BsonTimestamp timestamp = (BsonTimestamp) document.get(MongoMasterConnector.TS);
    return new MongoDataId(timestamp.getTime(), timestamp.getInc());
  }

  static BinlogInfo fromDataId(String dataId) {
    String[] split = dataId.split(SEP);
    if (split.length == 4 || split.length == 5) {
      return BinlogInfo.withFilenameCheck(split[0], Long.parseLong(split[1]));
    }
    throw new IllegalArgumentException(dataId);
  }

  static DocTimestamp fromMongoDataId(String dataId) {
    String[] split = dataId.split(SEP);
    if (split.length == 2
        || split.length == 3) { // for backward compatibility
      return new DocTimestamp(
          new BsonTimestamp(Integer.parseInt(split[0]), Integer.parseInt(split[1])));
    }
    throw new IllegalArgumentException(dataId);
  }


  String eventId();

  String dataId();

  boolean equals(Object o);

  SyncInitMeta getSyncInitMeta();

  String toString();

}
