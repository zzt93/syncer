package com.github.zzt93.syncer.common.data;

import com.github.zzt93.syncer.producer.input.mongo.DocTimestamp;
import org.bson.BsonTimestamp;

import java.util.Comparator;
import java.util.Objects;

/**
 * @author zzt
 */
public class MongoDataId implements DataId {
  private static final int COMMON_LEN = 15;

  private final int time;
  private final int inc;
  private Integer copy;

  public MongoDataId(int time, int inc) {
    this.time = time;
    this.inc = inc;
  }

  public MongoDataId setCopy(Integer copy) {
    this.copy = copy;
    return this;
  }

  @Override
  public String eventId() {
    return eventIdBuilder().toString();
  }

  private StringBuilder eventIdBuilder() {
    return new StringBuilder(COMMON_LEN).append(time).append(SEP).append(inc);
  }

  @Override
  public String dataId() {
    StringBuilder append = eventIdBuilder();
    if (copy != null) {
      return append.append(SEP).append(copy).toString();
    } else {
      return append.toString();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MongoDataId that = (MongoDataId) o;
    return time == that.time &&
        inc == that.inc &&
        Objects.equals(copy, that.copy);
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

    int cmp = getSyncInitMeta().compareTo(that.getSyncInitMeta());
    if(cmp != 0){
      return cmp;
    }
    return Objects.equals(copy, that.copy) ? 0 :
        copy == null ? -1 :
            that.copy == null ? 1 :
                Objects.compare(copy, that.copy, Comparator.naturalOrder());
  }

  @Override
  public MongoDataId copyAndCount(int copy) {
    return new MongoDataId(time, inc).setCopy(copy);
  }

  @Override
  public String toString() {
    return dataId();
  }

}
