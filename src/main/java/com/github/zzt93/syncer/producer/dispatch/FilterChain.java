package com.github.zzt93.syncer.producer.dispatch;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.zzt93.syncer.common.Filter.FilterRes;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.producer.input.meta.ConnectionSchemaMeta;
import com.github.zzt93.syncer.producer.output.OutputSink;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class FilterChain {

  private final Logger logger = LoggerFactory.getLogger(FilterChain.class);
  private final InputPipeHead start;
  private final OutputSink outputSink;

  public FilterChain(ConnectionSchemaMeta connectionSchemaMeta, OutputSink outputSink) {
    this.start = new InputPipeHead(connectionSchemaMeta);
    this.outputSink = outputSink;
  }

  public FilterRes decide(String eventId, Event[] events) {
    logger.debug("Receive binlog event: {}", Arrays.toString(events));
    SyncData[] aim = start.decide(eventId, events[0], events[1]);
    if (aim == null) { // not interested in this database+table
      return FilterRes.DENY;
    }

    FilterRes res = FilterRes.ACCEPT;
    try {
      outputSink.output(aim);
    } catch (Exception e) {
      res = FilterRes.DENY;
      logger.error("Fail to send data to output: {}", outputSink, e);
    }
    return res;
  }

}
