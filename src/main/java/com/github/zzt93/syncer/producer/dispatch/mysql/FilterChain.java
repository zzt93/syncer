package com.github.zzt93.syncer.producer.dispatch.mysql;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.zzt93.syncer.common.Filter.FilterRes;
import com.github.zzt93.syncer.common.data.SyncData;
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

  public FilterChain(ConsumerSchemaMeta consumerSchemaMeta, ProducerSink producerSink) {
    this.entry = new SchemaAndRowFilter(consumerSchemaMeta);
    this.producerSink = producerSink;
  }

  public FilterRes decide(String eventId, Event[] events) {
    if (logger.isDebugEnabled()) {
      logger.debug("Receive binlog event: {}", Arrays.toString(events));
    }
    SyncData[] aim = entry.decide(eventId, events[0], events[1]);
    if (aim == null) { // not interested in this database+table
      return FilterRes.DENY;
    }

    FilterRes res = FilterRes.ACCEPT;
    try {
      producerSink.output(aim);
    } catch (Exception e) {
      res = FilterRes.DENY;
      logger.error("Fail to send data to output: {}", producerSink, e);
    }
    return res;
  }

}
