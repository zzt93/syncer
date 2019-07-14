package com.github.zzt93.syncer.common.data;


import com.github.zzt93.syncer.config.consumer.input.MasterSourceType;
import com.github.zzt93.syncer.producer.input.mongo.DocTimestamp;
import com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo;

/**
 * @author zzt
 */
public interface SyncInitMeta<T> extends Comparable<T> {

  static SyncInitMeta earliest(MasterSourceType sourceType) {
    SyncInitMeta syncInitMeta;
    switch (sourceType) {
      case MySQL:
        syncInitMeta = BinlogInfo.earliest;
        break;
      case Mongo:
        syncInitMeta = DocTimestamp.earliest;
        break;
      default:
        throw new IllegalStateException("Not implement");
    }
    return syncInitMeta;
  }

  static SyncInitMeta latest(MasterSourceType sourceType) {
    SyncInitMeta syncInitMeta;
    switch (sourceType) {
      case MySQL:
        syncInitMeta = BinlogInfo.latest;
        break;
      case Mongo:
        syncInitMeta = DocTimestamp.latest;
        break;
      default:
        throw new IllegalStateException("Not implement");
    }
    return syncInitMeta;
  }

}
