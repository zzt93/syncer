package com.github.zzt93.syncer.producer.dispatch.mysql;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.zzt93.syncer.common.Filter.FilterRes;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.producer.input.mysql.meta.ConsumerSchemaMeta;
import com.github.zzt93.syncer.producer.output.ProducerSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * @author zzt
 */
public class FilterChain {

  private final Logger logger = LoggerFactory.getLogger(FilterChain.class);
  private final SchemaAndRowFilter entry;
  private final ProducerSink producerSink;

  FilterChain(ConsumerSchemaMeta consumerSchemaMeta, ProducerSink producerSink, boolean onlyUpdated) {
    this.entry = new SchemaAndRowFilter(consumerSchemaMeta, onlyUpdated);
    this.producerSink = producerSink;
  }

  FilterRes decide(SimpleEventType simpleEventType, String eventId, Event[] events) {
    if (logger.isDebugEnabled()) {
      logger.debug("Receive binlog event: {}", Arrays.toString(events));
    }
    SyncData[] aim = entry.decide(simpleEventType, eventId, events[0], events[1]);
    if (aim == null) { // not interested in this database+table
      return FilterRes.DENY;
    }

    boolean output = producerSink.output(aim);
    return output ? FilterRes.ACCEPT : FilterRes.DENY;
  }

}
