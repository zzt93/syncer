package com.github.zzt93.syncer.producer.input.mongo;

import com.github.zzt93.syncer.common.data.SyncInitMeta;
import org.bson.BsonTimestamp;

/**
 * @author zzt <a href="https://stackoverflow.com/questions/31057827/is-mongodb-id-objectid-generated-in-an-ascending-order">
 * Should not use doc _id</a>
 */
public class DocTimestamp implements SyncInitMeta<DocTimestamp> {

  /**
   * @see BsonTimestamp
   */
  public static final DocTimestamp earliest = new DocTimestamp(new BsonTimestamp(0));
  public static final DocTimestamp latest = new DocTimestamp(new BsonTimestamp((int) (System.currentTimeMillis() / 1000), 0));

  private final BsonTimestamp timestamp;

  public DocTimestamp(BsonTimestamp data) {
    timestamp = data;
  }

  BsonTimestamp getTimestamp() {
    return timestamp;
  }

  @Override
  public int compareTo(DocTimestamp o) {
    if (o == this) {
      return 0;
    }

    if (o == earliest) {
      return -1;
    } else if (o == latest) {
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
