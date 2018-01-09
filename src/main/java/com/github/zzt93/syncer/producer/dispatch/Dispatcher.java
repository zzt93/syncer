package com.github.zzt93.syncer.producer.dispatch;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.producer.output.OutputSink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class Dispatcher {

  private Logger logger = LoggerFactory.getLogger(Dispatcher.class);
  private HashMap<String, List<OutputSink>> map = new HashMap<>();

  public Dispatcher(Set<OutputSink> output) {
    for (OutputSink outputSink : output) {
      List<String> accepts = outputSink.accept();
      for (String accept : accepts) {
        map.computeIfAbsent(accept, k -> new ArrayList<>()).add(outputSink);
      }
    }
  }

  public boolean dispatch(SyncData[] syncData) {
    String dispatchStr = fromSyncData(syncData[0]);
    boolean res = true;
    for (OutputSink outputSink : map.get(dispatchStr)) {
      try {
        outputSink.output(syncData);
      } catch (Exception e) {
        res = false;
        logger.error("", e);
      }
    }
    return res;
  }

  private String fromSyncData(SyncData syncData) {
    // TODO 18/1/9
    return syncData.getTable();
  }

}
