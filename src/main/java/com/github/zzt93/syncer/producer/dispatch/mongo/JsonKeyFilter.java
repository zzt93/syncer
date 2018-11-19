package com.github.zzt93.syncer.producer.dispatch.mongo;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.pipeline.input.Schema;
import com.github.zzt93.syncer.producer.output.ProducerSink;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author zzt
 */
public class JsonKeyFilter {

  private final Schema schema;
  private final ProducerSink producerSink;

  public JsonKeyFilter(Schema schema, ProducerSink producerSink) {
    this.schema = schema;
    this.producerSink = producerSink;
  }

  public boolean output(SyncData data) {
    Set<String> tableRow = schema.getTableRow(data.getSchema(), data.getTable());
    HashMap<String, Object> records = data.getFields();
    HashSet<String> tmp = new HashSet<>();
    for (Entry<String, Object> entry : records.entrySet()) {
      if (!tableRow.contains(entry.getKey())) {
        tmp.add(entry.getKey());
      }
    }
    tmp.forEach(records::remove);
    if (records.isEmpty() && data.getType() == EventType.UPDATE_ROWS) {
      return false;
    }
    return producerSink.output(data);
  }
}
