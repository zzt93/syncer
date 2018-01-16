package com.github.zzt93.syncer.consumer;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.config.pipeline.common.Connection;
import com.github.zzt93.syncer.config.pipeline.input.Schema;
import com.github.zzt93.syncer.producer.input.connect.BinlogInfo;
import java.util.Set;

/**
 * @author zzt
 */
public interface InputSource extends Hashable {

  Connection getRemoteConnection();

  BinlogInfo getBinlogInfo();

  Set<Schema> getSchemas();

  String clientId();

  boolean input(SyncData data);

  boolean input(SyncData[] data);

  String toString();
}
