package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.common.ThreadSafe;
import com.github.zzt93.syncer.producer.input.connect.BinlogInfo;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author zzt
 */
public class Ack implements Callable<Void> {

  private AtomicReference<BinlogInfo> binlogInfo = new AtomicReference<>();

  @Override
  public Void call() throws Exception {
    return null;
  }

  @ThreadSafe(sharedBy = {"syncer-input: connect()", "shutdown hook"})
  List<String> connectorMeta() {
    BinlogInfo binlogInfo = this.binlogInfo.get();
    return Lists.newArrayList(binlogInfo.getBinlogFilename(), "" + binlogInfo.getBinlogPosition());
  }

}
