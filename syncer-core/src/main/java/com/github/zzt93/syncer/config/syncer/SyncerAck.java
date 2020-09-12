package com.github.zzt93.syncer.config.syncer;


import com.github.zzt93.syncer.config.common.EtcdConnection;
import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.TimeUnit;

/**
 * @author zzt
 */
@Getter
@Setter
public class SyncerAck {

  private int flushPeriod = 100;
  private TimeUnit unit = TimeUnit.MILLISECONDS;
  private EtcdConnection etcd;

}
