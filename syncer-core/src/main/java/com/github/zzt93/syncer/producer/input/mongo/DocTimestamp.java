package com.github.zzt93.syncer.producer.input.mongo;

import com.github.zzt93.syncer.common.data.SyncInitMeta;
import org.bson.BsonTimestamp;

/**
 * @author zzt <a href="https://stackoverflow.com/questions/31057827/is-mongodb-id-objectid-generated-in-an-ascending-order">
 * Should not use doc _id</a>
 */
public class DocTimestamp implements SyncInitMeta<DocTimestamp> {

  private final BsonTimestamp timestamp;

  public DocTimestamp(BsonTimestamp data) {
    timestamp = data;
  }

  public DocTimestamp() {
    timestamp = null;
  }

  public BsonTimestamp getTimestamp() {
    return timestamp;
  }

  @Override
  public int compareTo(DocTimestamp o) {
    if (timestamp == null && o.timestamp == null) {
      return 0;
    } else if (timestamp == null) {
      return -1;
    } else if (o.timestamp == null) {
      return 1;
    }
    return timestamp.compareTo(o.timestamp);
  }

  @Override
  public String toString() {
    return "DocTimestamp{" +
        "timestamp='" + timestamp + '\'' +
        '}';
  }
}
