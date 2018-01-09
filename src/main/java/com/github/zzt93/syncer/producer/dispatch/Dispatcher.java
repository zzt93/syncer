package com.github.zzt93.syncer.producer.dispatch;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.producer.output.OutputSink;
import java.util.HashMap;

/**
 * @author zzt
 */
public class Dispatcher {

  private HashMap<String, OutputSink> map = new HashMap<>();

  public boolean dispatch(SyncData[] syncData) {

  }

}
