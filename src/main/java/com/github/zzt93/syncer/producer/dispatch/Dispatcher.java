package com.github.zzt93.syncer.producer.dispatch;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.zzt93.syncer.common.Filter.FilterRes;
import com.github.zzt93.syncer.common.IdGenerator;
import com.github.zzt93.syncer.producer.input.meta.ConnectionSchemaMeta;
import com.github.zzt93.syncer.producer.output.OutputSink;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map.Entry;
import org.slf4j.MDC;

/**
 * @author zzt
 */
public class Dispatcher {

  private final List<FilterChain> filterChains;

  public Dispatcher(IdentityHashMap<ConnectionSchemaMeta, OutputSink> sinkHashMap) {
    filterChains = new ArrayList<>(sinkHashMap.size());
    for (Entry<ConnectionSchemaMeta, OutputSink> entry : sinkHashMap.entrySet()) {
      filterChains.add(new FilterChain(entry.getKey(), entry.getValue()));
    }
  }

  public boolean dispatch(Event... events) {
    String eventId = IdGenerator.fromEvent(events[1]);
    MDC.put(IdGenerator.EID, eventId);
    boolean res = true;
    for (FilterChain filterChain : filterChains) {
      FilterRes decide = filterChain.decide(events);
      res = res && FilterRes.ACCEPT == decide;
    }
    MDC.remove(IdGenerator.EID);
    return res;
  }

}
