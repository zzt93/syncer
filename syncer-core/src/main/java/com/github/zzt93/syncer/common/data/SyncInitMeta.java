package com.github.zzt93.syncer.common.data;


import com.github.zzt93.syncer.config.pipeline.input.MasterSourceType;
import com.github.zzt93.syncer.producer.input.mongo.DocTimestamp;
import com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo;

/**
 * @author zzt
 */
public interface SyncInitMeta<T> extends Comparable<T> {

  static SyncInitMeta defaultMeta(MasterSourceType sourceType) {
    SyncInitMeta syncInitMeta = null;
    switch (sourceType) {
      case MySQL:
        syncInitMeta= new BinlogInfo();
        break;
      case Mongo:
        syncInitMeta = new DocTimestamp();
        break;
    }
    return syncInitMeta;
  }

}
