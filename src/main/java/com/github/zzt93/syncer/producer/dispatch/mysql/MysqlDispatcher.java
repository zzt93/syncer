package com.github.zzt93.syncer.producer.dispatch.mysql;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.zzt93.syncer.common.Filter.FilterRes;
import com.github.zzt93.syncer.common.IdGenerator;
import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import com.github.zzt93.syncer.producer.dispatch.Dispatcher;
import com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo;
import com.github.zzt93.syncer.producer.input.mysql.meta.ConnectionSchemaMeta;
import com.github.zzt93.syncer.producer.output.OutputSink;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * @author zzt
 */
public class MysqlDispatcher implements Dispatcher {

  private final List<FilterChain> filterChains;
  private final AtomicReference<BinlogInfo> binlogInfo;
  private final Logger logger = LoggerFactory.getLogger(MysqlDispatcher.class);

  public MysqlDispatcher(IdentityHashMap<ConnectionSchemaMeta, OutputSink> sinkHashMap,
      AtomicReference<BinlogInfo> binlogInfo) {
    filterChains = new ArrayList<>(sinkHashMap.size());
    this.binlogInfo = binlogInfo;
    if (sinkHashMap.isEmpty()) {
      logger.error("No dispatch info fetched: no meta info dispatcher & output sink");
      throw new InvalidConfigException("Invalid address & schema & table config");
    }
    for (Entry<ConnectionSchemaMeta, OutputSink> entry : sinkHashMap.entrySet()) {
      filterChains.add(new FilterChain(entry.getKey(), entry.getValue()));
    }
  }

  @Override
  public boolean dispatch(Object... data) {
    Preconditions.checkState(data.length == 2);
    Event[] events = new Event[]{(Event) data[0], (Event) data[1]};
    String eventId = IdGenerator.fromEvent(events, binlogInfo.get().getBinlogFilename());
    MDC.put(IdGenerator.EID, eventId);
    boolean res = true;
    for (FilterChain filterChain : filterChains) {
      FilterRes decide = filterChain.decide(eventId, events);
      res = res && FilterRes.ACCEPT == decide;
    }
    MDC.remove(IdGenerator.EID);
    return res;
  }

}
