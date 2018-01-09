package com.github.zzt93.syncer.consumer;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.config.pipeline.input.Schema;
import com.github.zzt93.syncer.producer.input.connect.BinlogInfo;
import java.util.List;

/**
 * @author zzt
 */
public interface InputSource extends Hashable {

  /**
   * register input and output
   * @return whether registration is success
   */
  boolean register();

  BinlogInfo getBinlogInfo();

  List<Schema> getSchemas();

  String clientId();

  boolean input(SyncData data);

  boolean input(SyncData[] data);

}
