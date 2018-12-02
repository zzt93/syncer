package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo;

/**
 * @author zzt
 */
public interface MysqlInputSource {

  BinlogInfo getSyncInitMeta();

}
