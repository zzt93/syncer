package com.github.zzt93.syncer.producer.input.mongo;

import com.github.zzt93.syncer.common.data.SyncInitMeta;
import org.bson.types.BSONTimestamp;

/**
 * @author zzt
 */
public class DocTimestamp implements SyncInitMeta<DocTimestamp> {

  private final BSONTimestamp timestamp;

  public DocTimestamp(BSONTimestamp data) {
    timestamp = data;
  }

  public BSONTimestamp getTimestamp() {
    return timestamp;
  }

  @Override
  public int compareTo(DocTimestamp o) {
    return timestamp.compareTo(o.timestamp);
  }

  @Override
  public String toString() {
    return "DocTimestamp{" +
        "timestamp='" + timestamp + '\'' +
        '}';
  }
}
