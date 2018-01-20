package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.producer.input.mongo.DocId;

/**
 * @author zzt
 */
public interface MongoInputSource {

  DocId getSyncInitMeta();

}
