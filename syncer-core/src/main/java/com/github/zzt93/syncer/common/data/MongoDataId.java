package com.github.zzt93.syncer.common.data;

import com.github.zzt93.syncer.producer.input.mongo.DocTimestamp;
import org.bson.BsonTimestamp;

import java.util.Objects;

/**
 * @author zzt
 */
public class MongoDataId implements DataId {
  private static final int COMMON_LEN = 15;

  private final int time;
  private final int inc;

  public MongoDataId(int time, int inc) {
    this.time = time;
    this.inc = inc;
  }

  @Override
  public String eventId() {
    return new StringBuilder(COMMON_LEN).append(time).append(SEP).append(inc).toString();
  }

  @Override
  public String dataId() {
    return eventId();

  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MongoDataId that = (MongoDataId) o;
    return time == that.time &&
        inc == that.inc;
  }

  @Override
  public int hashCode() {
    return Objects.hash(time, inc);
  }

  @Override
  public SyncInitMeta getSyncInitMeta() {
    return new DocTimestamp(new BsonTimestamp(time, inc));
  }

  @Override
  public int compareTo(DataId o) {
    MongoDataId that = (MongoDataId) o;

    return getSyncInitMeta().compareTo(that.getSyncInitMeta());
  }

  @Override
  public String toString() {
    return dataId();
  }

  public enum Offset {
    DUP(), CLONE()
  }
}
