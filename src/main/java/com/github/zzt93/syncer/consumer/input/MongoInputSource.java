package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.producer.input.mongo.DocTimestamp;

/**
 * @author zzt
 */
public interface MongoInputSource {

  DocTimestamp getSyncInitMeta();

}
